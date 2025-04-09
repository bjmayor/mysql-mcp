use sqlx::{
    Connection, Executor, MySqlConnection, MySqlPool,
    migrate::{MigrationSource, Migrator},
};
use std::{path::Path, thread};
use tokio::runtime::Runtime;
use uuid::Uuid;

#[derive(Debug)]
pub struct TestMysql {
    pub server_url: String,
    pub dbname: String,
}

impl TestMysql {
    pub fn new<S>(server_url: impl Into<String>, migrations: S) -> Self
    where
        S: MigrationSource<'static> + Send + Sync + 'static,
    {
        let server_url = server_url.into();

        let (base_server_url, parsed_dbname) = parse_mysql_url(&server_url);

        let uuid = Uuid::new_v4();
        let simple = uuid.to_string();

        let dbname = match parsed_dbname {
            Some(db_name) => format!("{}_test_{}", db_name, simple),
            None => format!("test_{}", simple),
        };
        let dbname_cloned = dbname.clone();

        let tdb = Self {
            server_url: base_server_url,
            dbname,
        };

        let url = tdb.url();

        // create database dbname
        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                // use server url to create database
                let mut conn = MySqlConnection::connect(&server_url).await.unwrap();
                conn.execute(format!(r#"CREATE DATABASE `{}`"#, dbname_cloned).as_str())
                    .await
                    .unwrap();

                // now connect to test database for migration
                let mut conn = MySqlConnection::connect(&url).await.unwrap();
                let m = Migrator::new(migrations).await.unwrap();
                m.run(&mut conn).await.unwrap();
            });
        })
        .join()
        .expect("failed to create database");

        tdb
    }

    pub fn url(&self) -> String {
        format!("{}/{}", self.server_url, self.dbname)
    }

    pub async fn get_pool(&self) -> MySqlPool {
        MySqlPool::connect(&self.url()).await.unwrap()
    }
}

impl Drop for TestMysql {
    fn drop(&mut self) {
        let server_url = self.server_url.clone();
        let dbname = self.dbname.clone();
        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async move {
                let mut conn = MySqlConnection::connect(&server_url).await.unwrap();
                // TODO: terminate existing connections
                conn.execute(format!(r#"DROP DATABASE `{}`"#, dbname).as_str())
                    .await
                    .expect("Error while querying the drop database");
            });
        })
        .join()
        .expect("failed to drop database");
    }
}

impl Default for TestMysql {
    fn default() -> Self {
        Self::new(
            "mysql://root:123456@localhost:3306",
            Path::new("./fixtures/mysql-migrations"),
        )
    }
}

/// 解析 MySQL URL，返回服务器 URL 和可选的数据库名称
///
/// # 参数
///
/// * `url` - MySQL 连接字符串
///
/// # 返回
///
/// 返回一个元组，包含服务器 URL 和可选的数据库名称
fn parse_mysql_url(url: &str) -> (String, Option<String>) {
    let url_without_protocol = url.trim_start_matches("mysql://");

    let parts: Vec<&str> = url_without_protocol.split('/').collect();
    let server_url = format!("mysql://{}", parts[0]);

    let dbname = if parts.len() > 1 && !parts[1].is_empty() {
        Some(parts[1].to_string())
    } else {
        None
    };

    (server_url, dbname)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mysql_should_create_and_drop() {
        let tdb = TestMysql::default();
        let pool = tdb.get_pool().await;
        // insert todo
        sqlx::query("INSERT INTO todos (title) VALUES ('test')")
            .execute(&pool)
            .await
            .unwrap();
        // get todo
        let (id, title) = sqlx::query_as::<_, (i32, String)>("SELECT id, title FROM todos")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(title, "test");
    }
}
