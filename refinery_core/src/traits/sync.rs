use crate::error::WrapMigrationError;
use crate::traits::{
    insert_migration_query, verify_migrations, GET_APPLIED_MIGRATIONS_QUERY,
    GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::{Error, Migration, Report, Target};

pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute<'a, S: AsRef<str>, T: Iterator<Item = S>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error>;
}

pub trait Query<T>: Transaction {
    fn query(&mut self, query: &str) -> Result<T, Self::Error>;
}

fn migration_whether_apply(migration: &Migration, target: Target) -> bool {
    if let Target::Version(input_target) | Target::FakeVersion(input_target) = target {
        if input_target < migration.version() && *migration.prefix() != crate::runner::Type::Rerunnable {
            let migration_name = migration.name();
            log::info!(
                "skipping migration: {migration_name}, due to user option {input_target}",
            );
            return false;
        }
    }
    return true;
}

pub struct MigrateReusableIteratorArgs<'mtn> {
    migrations: Vec<Migration>,
    target: Target,
    batched: bool,
    migration_table_name: &'mtn str
}

pub(crate) struct MigrateReusableIterator<'mtn> {
    args: MigrateReusableIteratorArgs<'mtn>,

    iter_state: u32,
    iter_state_nested: u8
}

use std::iter::{Flatten, Map, Filter};
use std::slice::Iter;
use itertools::Format;
use std::borrow::Cow;
pub(crate) struct MigrateReusableIteratorItem<'s, MapperA, BatchedMapperB, MigrationsShouldApply > 
 where
    MapperA: FnMut(&'s Migration) -> (&'s Migration, (Option<&'s str>, Option<String>)),
    BatchedMapperB: FnMut((&'s Migration, (Option<&'s str>, Option<String>))) -> [Option<Cow<'s, str>>; 2],
    MigrationsShouldApply: for <'a> FnMut(&'a &'s Migration) -> bool,
{
    pub(crate) log_before_tx: LogData,
    pub(crate) applied_migrations: Filter<std::slice::Iter<'s, Migration>, MigrationsShouldApply>,
    pub(crate) result: MigrateReusableResult<'s, MapperA, BatchedMapperB, MigrationsShouldApply>
}

pub(crate) struct LogData {
    pub(crate) level: log::Level,
    pub(crate) msg: &'static str,
}
pub(crate) enum MigrateReusableResult<'s, MapperA, BatchedMapperB, MigrationsShouldApply>  
where
    MapperA: FnMut(&'s Migration) -> (&'s Migration, (Option<&'s str>, Option<String>)),
    BatchedMapperB: FnMut((&'s Migration, (Option<&'s str>, Option<String>))) -> [Option<Cow<'s, str>>; 2],
    MigrationsShouldApply: for <'a> FnMut(&'a &'s Migration) -> bool,
{
    Batched {
        sql: Flatten<
            Flatten<
                Map<
                    Map<
                        Filter<
                            Iter<'s, Migration>,
                            MigrationsShouldApply
                        >,
                        MapperA
                    >,
                    BatchedMapperB
                >
            >
        >,

        migrations_display: Format<'s, Filter<Iter<'s, Migration>, MigrationsShouldApply>> 
    },
    Itemized {
        sql: &'s str,
        current_migration: &'s Migration
    },
    ItemizedMetaInsert {
        sql: String,
        current_migration: &'s Migration
    }
}

impl<'mtn> MigrateReusableIterator<'mtn> 
{
    pub(crate) fn next<'s>(&'s mut self) -> Option<
        MigrateReusableIteratorItem<
            's, 
            impl for<'a> FnMut(&'a Migration) -> (&'a Migration, (Option<&'a str>, Option<String>)) +'s,
            impl for <'a> FnMut((&'a Migration, (Option<&'a str>, Option<String>))) -> [Option<Cow<'a, str>>; 2] +'s,
            impl for <'a, 'b> FnMut(&'b &'a Migration) -> bool + 's,
        >   
    > 

    {
        let migrations_checked_skip = &self.args.migrations[self.iter_state as usize..];

        fn constrain<F>(f: F) -> F 
        where F: for<'a> FnMut(&'a Migration) -> (&'a Migration, (Option<&'a str>, Option<String>)) 
        { f }

        let target = self.args.target;

        let filter = move |migration: &&Migration| {
            migration_whether_apply(migration, target)
        };
        let migration_table_name = self.args.migration_table_name;
        let target = self.args.target;

        let mut migrations_filtered_by_whether_apply = migrations_checked_skip.iter().filter(filter).map(constrain(move |migration: &Migration| {
            let migration_sql = migration.sql().expect("sql must be Some!");
            let insert_into_migrations_table = insert_migration_query(&migration, migration_table_name);

            // If Target is Fake, we only update schema migrations table
            if !matches!(target, Target::Fake | Target::FakeVersion(_)) {
                (migration, (Some(migration_sql), Some(insert_into_migrations_table)))
            } else {
                (migration, (None, Some(insert_into_migrations_table)))
            }
        }));

        let migrations_to_apply = || migrations_checked_skip.iter().filter(filter);

        let migrations_applied_for_logging = self.args.migrations[..self.iter_state as usize].iter().filter(filter);

        let next_maybe_batched_or_itemized = if self.args.batched {
            if self.iter_state == 0 {
                fn constrain<F>(f: F) -> F 
                where F: for<'a> FnMut((&'a Migration, (Option<&'a str>, Option<String>))) -> [Option<Cow<'a, str>>; 2] 
                { f }
                let strings_only = migrations_filtered_by_whether_apply.map(constrain(move |(_, (reference, owned)): (&Migration, (Option<&str>, Option<String>))| [reference.map(Cow::Borrowed),owned.map(Cow::Owned)])).flatten().flatten();
                let migrations_display = itertools::Itertools::format(migrations_to_apply(), ", ");

                Some(
                        MigrateReusableIteratorItem {
                            log_before_tx: LogData {level: log::Level::Info, msg: "going to apply batch migrations in single transaction"},
                            applied_migrations: migrations_applied_for_logging,
                            result: MigrateReusableResult::Batched {
                                sql: strings_only,
                                migrations_display
                            }
                        }
                )
            } else {
                None
            }
        } else {
            let next_maybe_from_nested_or_from_next_migration = loop {
                let next_maybe_migration_or_insert_into_migrations_table = match migrations_filtered_by_whether_apply.next() {
                    Some((current_migration_struct, (migration_sql, migration_table_insert_sql))) => {
                        match (migration_sql, migration_table_insert_sql, self.iter_state_nested) {
                            (Some(migration_sql), Some(_), 0) => {
                                Some(
                                    MigrateReusableIteratorItem {
                                        log_before_tx: LogData {level: log::Level::Info, msg: "applying migration"},
                                        applied_migrations: migrations_applied_for_logging,
                                        result: MigrateReusableResult::Itemized {
                                            sql: migration_sql,
                                            current_migration: current_migration_struct,
                                        }
                                    }
                                )
                            }
                            (None, Some(migrations_table_insert_sql), 0) | (Some(_), Some(migrations_table_insert_sql), 1) => {
                                Some(
                                    MigrateReusableIteratorItem {
                                        log_before_tx: LogData {level: log::Level::Debug, msg: "applied migration, writing state to db"},
                                        applied_migrations: migrations_applied_for_logging,

                                        result: MigrateReusableResult::ItemizedMetaInsert {
                                            sql: migrations_table_insert_sql,
                                            current_migration: current_migration_struct,
                                        }
                                    }
                                )
                            }
                            (None, Some(_), 1..) | (Some(_), Some(_), 2..) => {
                                // Exhausted pair, move on to next migration
                                self.iter_state_nested = 0;
                                self.iter_state += 1;
                                continue;
                            },
                            _ => unreachable!()
                        }
                    }
                    // Actual end of underlying migration stream, return None, i.e. close iterator
                    None => break None
                };
                self.iter_state_nested += 1;

                break next_maybe_migration_or_insert_into_migrations_table
            };
            next_maybe_from_nested_or_from_next_migration
        };

        next_maybe_batched_or_itemized
    }
    pub(crate) fn applied(self) -> Vec<Migration> {
        self.args.migrations.into_iter().filter(|migration| migration_whether_apply(migration, self.args.target)).collect::<Vec<_>>()
    }
}


impl<'mtn> MigrateReusableIterator<'mtn> {
    fn new(args: MigrateReusableIteratorArgs<'mtn>) -> Self {
        Self {
            args,
            iter_state: 0,
            iter_state_nested: 0
        }
    }
}

pub(crate) fn migrate_reusable<'mtn>(
    mut migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &'mtn str,
    batched: bool,
) -> MigrateReusableIterator<'mtn> {
    let migrations_count = migrations.iter_mut().map(|migration| {
        if migration_whether_apply(&migration, target) {
            migration.set_applied();
        }
    }).count();

    match (target, batched) {
        (Target::Fake | Target::FakeVersion(_), _) => {
            log::info!("not going to apply any migration as fake flag is enabled.");
        }
        (Target::Latest | Target::Version(_), true) => {
            log::info!(
                "going to batch apply {} migrations in single transaction.",
                migrations_count
            );
        }
        (Target::Latest | Target::Version(_), false) => {
            log::info!(
                "going to apply {} migrations in multiple transactions.",
                migrations_count,
            );
        }
    };

    MigrateReusableIterator::new(MigrateReusableIteratorArgs {
        migrations, target, batched, migration_table_name
    })
}

pub fn migrate<T: Transaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &str,
    batched: bool,
) -> Result<Report, Error> {
    let mut iter = migrate_reusable(migrations, target, migration_table_name, batched);
    while let Some(next) = iter.next() {
        match next.result {
            MigrateReusableResult::Batched { sql, migrations_display } => {
                log::log!(next.log_before_tx.level, "{}:\n{migrations_display}", next.log_before_tx.msg);
                transaction
                    .execute(sql)
                    .migration_err("error applying batch migration", || [].into_iter())?;
            },
            MigrateReusableResult::Itemized { sql, current_migration } => {
                log::log!(next.log_before_tx.level, "{}: {current_migration}", next.log_before_tx.msg);
                transaction
                    .execute([sql].into_iter())
                    .migration_err("error applying single migration", || next.applied_migrations.cloned())?;
            }
            MigrateReusableResult::ItemizedMetaInsert { sql, current_migration } => {
                log::log!(next.log_before_tx.level, "{}: {current_migration}", next.log_before_tx.msg);
                transaction
                    .execute([sql].into_iter())
                    .migration_err("error applying update", || next.applied_migrations.cloned())?;
            }
        }
    }

    Ok(Report::new(iter.applied()))
}

pub trait Migrate: Query<Vec<Migration>>
where
    Self: Sized,
{
    // Needed cause some database vendors like Mssql have a non sql standard way of checking the migrations table
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        super::assert_migrations_table_query(migration_table_name)
    }

    fn get_last_applied_migration_query(migration_table_name: &str) -> String {
        GET_LAST_APPLIED_MIGRATION_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    fn get_applied_migrations_query(migration_table_name: &str) -> String {
        GET_APPLIED_MIGRATIONS_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    fn assert_migrations_table(&mut self, migration_table_name: &str) -> Result<usize, Error> {
        // Needed cause some database vendors like Mssql have a non sql standard way of checking the migrations table,
        // though on this case it's just to be consistent with the async trait `AsyncMigrate`
        self.execute(
            [Self::assert_migrations_table_query(migration_table_name)].into_iter(),
        )
        .migration_err("error asserting migrations table", || [].into_iter())
    }

    fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        let mut migrations = self
            .query(Self::get_last_applied_migration_query(migration_table_name).as_str())
            .migration_err("error getting last applied migration", || [].into_iter())?;

        Ok(migrations.pop())
    }

    fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        let migrations = self
            .query(Self::get_applied_migrations_query(migration_table_name).as_str())
            .migration_err("error getting applied migrations", || [].into_iter())?;

        Ok(migrations)
    }

    fn get_unapplied_migrations(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        self.assert_migrations_table(migration_table_name)?;

        let applied_migrations = self.get_applied_migrations(migration_table_name)?;

        let migrations = verify_migrations(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing,
        )?;

        if migrations.is_empty() {
            log::info!("no migrations to apply");
        }

        Ok(migrations)
    }

    fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
        target: Target,
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        let migrations = self.get_unapplied_migrations(
            migrations,
            abort_divergent,
            abort_missing,
            migration_table_name,
        )?;

        if grouped || matches!(target, Target::Fake | Target::FakeVersion(_)) {
            migrate(self, migrations, target, migration_table_name, true)
        } else {
            migrate(self, migrations, target, migration_table_name, false)
        }
    }
}
