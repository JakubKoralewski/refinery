use crate::error::WrapMigrationError;
use crate::traits::{
    verify_migrations, GET_APPLIED_MIGRATIONS_QUERY,
    GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::{Error, Migration, Report, Target};

use async_trait::async_trait;

#[async_trait]
pub trait AsyncTransaction {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn execute<'a, S: AsRef<str> + Send, T: Iterator<Item = S> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error>;
}

#[async_trait]
pub trait AsyncQuery<T>: AsyncTransaction {
    async fn query(&mut self, query: &str) -> Result<T, Self::Error>;
}

async fn migrate_inner<T: AsyncTransaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &str,
    batched: bool
) -> Result<Report, Error> {
    let mut iter = crate::traits::sync::migrate_reusable(migrations, target, migration_table_name, batched);

    while let Some(migration) = iter.next() {
        use crate::traits::sync::MigrateReusableResult;
        match migration.result {
            MigrateReusableResult::Batched { sql, migrations_display } => {
                log::log!(migration.log_before_tx.level, "{}\n{migrations_display}", migration.log_before_tx.msg);
                transaction
                    .execute(sql)
                    .await
                    .migration_err(|| "error applying batch migration async", || [].into_iter())?;
            },
            MigrateReusableResult::Itemized { sql, current_migration } => {
                log::log!(migration.log_before_tx.level, "{}: {current_migration}", migration.log_before_tx.msg);
                transaction
                    .execute([sql].into_iter())
                    .await
                    .migration_err(|| format!("error applying single migration async: {current_migration}"), || migration.applied_migrations.cloned())?;
            }
            MigrateReusableResult::ItemizedMetaInsert { sql, current_migration } => {
                log::log!(migration.log_before_tx.level, "{}: {current_migration}", migration.log_before_tx.msg);
                transaction
                    .execute([sql].into_iter())
                    .await
                    .migration_err(|| format!("error applying update async: {current_migration}"), || migration.applied_migrations.cloned())?;
            }
        }
    }
    Ok(Report::new(iter.applied()))
}

#[async_trait]
pub trait AsyncMigrate: AsyncQuery<Vec<Migration>>
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

    async fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        let mut migrations = self
            .query(Self::get_last_applied_migration_query(migration_table_name).as_ref())
            .await
            .migration_err(|| "error getting last applied migration", || [].into_iter())?;

        Ok(migrations.pop())
    }

    async fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        let migrations = self
            .query(Self::get_applied_migrations_query(migration_table_name).as_ref())
            .await
            .migration_err(|| "error getting applied migrations", || [].into_iter())?;

        Ok(migrations)
    }

    async fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
        target: Target,
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        self.execute(
            [Self::assert_migrations_table_query(migration_table_name)].into_iter(),
        )
        .await
        .migration_err(|| "error asserting migrations table", || [].into_iter())?;

        let applied_migrations = self
            .get_applied_migrations(migration_table_name)
            .await
            .migration_err(|| "error getting current schema version", || [].into_iter())?;

        let migrations = verify_migrations(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing,
        )?;

        if migrations.is_empty() {
            log::info!("no migrations to apply");
        }

        let if_batched = grouped || matches!(target, Target::Fake | Target::FakeVersion(_));

        migrate_inner(self, migrations, target, migration_table_name, if_batched).await
    }
}
