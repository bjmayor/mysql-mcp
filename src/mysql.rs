use anyhow::Error;
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlparser::ast::Statement;
use sqlx::mysql::MySqlPool;
use sqlx::{Column, Row};
use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Conn {
    pub(crate) id: String,
    pub(crate) conn_str: String,
    pub(crate) pool: MySqlPool,
}

#[derive(Debug, Clone)]
pub struct Conns {
    pub(crate) inner: Arc<ArcSwap<HashMap<String, Conn>>>,
}

#[derive(Debug, Clone)]
pub struct MySqlMcp {
    pub(crate) conns: Conns,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize)]
struct JsonRow {
    ret: sqlx::types::Json<serde_json::Value>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize)]
struct ColumnInfo {
    column_name: String,
    data_type: String,
    character_maximum_length: Option<i64>,
    column_default: Option<String>,
    is_nullable: String,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize)]
struct TableInfo {
    table_name: String,
}

impl Conns {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(Arc::new(HashMap::new()))),
        }
    }

    pub(crate) async fn register(&self, conn_str: String) -> Result<String, Error> {
        let pool = MySqlPool::connect(&conn_str).await?;
        let id = uuid::Uuid::new_v4().to_string();
        let conn = Conn {
            id: id.clone(),
            conn_str: conn_str.clone(),
            pool,
        };

        let mut conns = self.inner.load().as_ref().clone();
        conns.insert(id.clone(), conn);
        self.inner.store(Arc::new(conns));

        Ok(id)
    }

    pub(crate) fn unregister(&self, id: String) -> Result<(), Error> {
        let mut conns = self.inner.load().as_ref().clone();
        if conns.remove(&id).is_none() {
            return Err(anyhow::anyhow!("Connection not found"));
        }
        self.inner.store(Arc::new(conns));
        Ok(())
    }

    pub(crate) async fn query(&self, id: &str, query: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let parsed_query = validate_sql(
            query,
            |stmt| matches!(stmt, Statement::Query(_)),
            "Only SELECT queries are allowed",
        )?;

        let rows = sqlx::query(&parsed_query).fetch_all(&conn.pool).await?;

        let mut results = Vec::new();
        for row in rows {
            let mut map = serde_json::Map::new();
            for i in 0..row.columns().len() {
                let column = &row.columns()[i];
                let value = match row.try_get::<serde_json::Value, _>(i) {
                    Ok(val) => val,
                    Err(_) => match row.try_get::<String, _>(i) {
                        Ok(s) => json!(s),
                        Err(_) => serde_json::Value::Null,
                    },
                };
                map.insert(column.name().to_string(), value);
            }
            results.push(serde_json::Value::Object(map));
        }

        Ok(serde_json::to_string(&results)?)
    }

    pub(crate) async fn insert(&self, id: &str, query: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = validate_sql(
            query,
            |stmt| matches!(stmt, Statement::Insert { .. }),
            "Only INSERT statements are allowed",
        )?;

        let result = sqlx::query(&query).execute(&conn.pool).await?;

        Ok(format!(
            "success, rows_affected: {}",
            result.rows_affected()
        ))
    }

    pub(crate) async fn update(&self, id: &str, query: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = validate_sql(
            query,
            |stmt| matches!(stmt, Statement::Update { .. }),
            "Only UPDATE statements are allowed",
        )?;

        let result = sqlx::query(&query).execute(&conn.pool).await?;

        Ok(format!(
            "success, rows_affected: {}",
            result.rows_affected()
        ))
    }

    pub(crate) async fn delete(&self, id: &str, query: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = validate_sql(
            query,
            |stmt| matches!(stmt, Statement::Delete { .. }),
            "Only DELETE statements are allowed",
        )?;

        let result = sqlx::query(&query).execute(&conn.pool).await?;

        Ok(format!(
            "success, rows_affected: {}",
            result.rows_affected()
        ))
    }

    pub(crate) async fn create_table(&self, id: &str, query: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = validate_sql(
            query,
            |stmt| matches!(stmt, Statement::CreateTable { .. }),
            "Only CREATE TABLE statements are allowed",
        )?;

        sqlx::query(&query).execute(&conn.pool).await?;

        Ok("success".to_string())
    }

    pub(crate) async fn drop_table(&self, id: &str, table: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = format!("DROP TABLE IF EXISTS `{}`", table);
        sqlx::query(&query).execute(&conn.pool).await?;

        Ok("success".to_string())
    }

    pub(crate) async fn create_index(&self, id: &str, query: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = validate_sql(
            query,
            |stmt| matches!(stmt, Statement::CreateIndex { .. }),
            "Only CREATE INDEX statements are allowed",
        )?;

        sqlx::query(&query).execute(&conn.pool).await?;

        Ok("success".to_string())
    }

    pub(crate) async fn drop_index(
        &self,
        id: &str,
        index: &str,
        table: &str,
    ) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = format!("DROP INDEX `{}` ON `{}`", index, table);
        sqlx::query(&query).execute(&conn.pool).await?;

        Ok("success".to_string())
    }

    pub(crate) async fn describe(&self, id: &str, table: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = r#"
          SELECT
            COLUMN_NAME as column_name,
            CAST(DATA_TYPE AS CHAR) as data_type,
            CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
            CAST(COLUMN_DEFAULT AS CHAR) as column_default,
            IS_NULLABLE as is_nullable
          FROM information_schema.columns
          WHERE table_schema = DATABASE() AND table_name = ?
          ORDER BY ordinal_position
        "#;

        let columns_info = sqlx::query_as::<_, ColumnInfo>(query)
            .bind(table)
            .fetch_all(&conn.pool)
            .await?;

        Ok(serde_json::to_string(&columns_info)?)
    }

    pub(crate) async fn list_tables(&self, id: &str, schema: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = r#"
          SELECT
            TABLE_NAME as table_name
          FROM information_schema.tables
          WHERE
            TABLE_SCHEMA = ?
            AND TABLE_TYPE = 'BASE TABLE'
          ORDER BY TABLE_NAME
        "#;

        let tables_info: Vec<TableInfo> = sqlx::query_as::<_, TableInfo>(query)
            .bind(schema)
            .fetch_all(&conn.pool)
            .await?;

        Ok(serde_json::to_string(&tables_info)?)
    }

    pub(crate) async fn create_schema(&self, id: &str, schema_name: &str) -> Result<String, Error> {
        let conns = self.inner.load();
        let conn = conns
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Connection not found"))?;

        let query = format!("CREATE DATABASE IF NOT EXISTS `{}`", schema_name);
        sqlx::query(&query).execute(&conn.pool).await?;

        Ok("success".to_string())
    }
}

impl Default for Conns {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_sql<F>(query: &str, validator: F, error_msg: &'static str) -> Result<String, Error>
where
    F: Fn(&Statement) -> bool,
{
    let dialect = sqlparser::dialect::MySqlDialect {};
    let statements = sqlparser::parser::Parser::parse_sql(&dialect, query)?;

    if statements.len() != 1 {
        return Err(anyhow::anyhow!("Only single statement queries are allowed"));
    }

    let statement = &statements[0];
    if validator(statement) {
        Ok(query.to_string())
    } else {
        Err(anyhow::anyhow!("{}", error_msg))
    }
}

#[cfg(test)]
mod tests {
    use crate::TestMysql;

    use super::*;

    const TEST_CONN_STR: &str = "mysql://root:123456@localhost:3306";

    async fn setup_test_db() -> (TestMysql, String) {
        let tdb = TestMysql::new(
            TEST_CONN_STR.to_string(),
            std::path::Path::new("./fixtures/migrations"),
        );
        let pool = tdb.get_pool().await;

        sqlx::query("CREATE TABLE IF NOT EXISTS test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            .execute(&pool)
            .await
            .unwrap();

        let conn_str = tdb.url();

        (tdb, conn_str)
    }

    #[tokio::test]
    async fn register_unregister_should_work() {
        let (_tdb, conn_str) = setup_test_db().await;
        let conns = Conns::new();

        let id = conns.register(conn_str.clone()).await.unwrap();
        assert!(!id.is_empty());

        assert!(conns.unregister(id.clone()).is_ok());
        assert!(conns.unregister(id).is_err());
    }

    #[tokio::test]
    async fn list_tables_describe_should_work() {
        let (_tdb, conn_str) = setup_test_db().await;
        let conns = Conns::new();
        let id = conns.register(conn_str).await.unwrap();

        let tables = conns.list_tables(&id, _tdb.dbname.as_str()).await.unwrap();
        assert!(tables.contains("test_table"));

        let description = conns.describe(&id, "test_table").await.unwrap();
        assert!(description.contains("id"));
        assert!(description.contains("name"));
        assert!(description.contains("created_at"));
    }

    #[tokio::test]
    async fn create_table_drop_table_should_work() {
        let (_tdb, conn_str) = setup_test_db().await;
        let conns = Conns::new();
        let id = conns.register(conn_str).await.unwrap();

        let create_table =
            "CREATE TABLE test_table2 (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))";
        assert_eq!(
            conns.create_table(&id, create_table).await.unwrap(),
            "success"
        );

        assert_eq!(
            conns.drop_table(&id, "test_table2").await.unwrap(),
            "success"
        );
    }

    #[tokio::test]
    async fn query_insert_update_delete_should_work() {
        let (_tdb, conn_str) = setup_test_db().await;
        let conns = Conns::new();
        let id = conns.register(conn_str).await.unwrap();

        let query = "SELECT * FROM test_table ORDER BY id";
        let result = conns.query(&id, query).await.unwrap();
        assert!(result.contains("test1"));
        assert!(result.contains("test2"));
        assert!(result.contains("test3"));

        let insert = "INSERT INTO test_table (name) VALUES ('test4')";
        let result = conns.insert(&id, insert).await.unwrap();
        assert!(result.contains("rows_affected: 1"));

        let update = "UPDATE test_table SET name = 'updated' WHERE name = 'test1'";
        let result = conns.update(&id, update).await.unwrap();
        assert!(result.contains("rows_affected: 1"));

        let result = conns
            .delete(&id, "DELETE FROM test_table WHERE name = 'updated'")
            .await
            .unwrap();
        assert!(result.contains("rows_affected: 1"));
    }

    #[tokio::test]
    async fn create_index_drop_index_should_work() {
        let (_tdb, conn_str) = setup_test_db().await;
        let conns = Conns::new();
        let id = conns.register(conn_str).await.unwrap();

        let create_index = "CREATE INDEX idx_test_table_new ON test_table (name, created_at)";
        assert_eq!(
            conns.create_index(&id, create_index).await.unwrap(),
            "success"
        );

        assert_eq!(
            conns
                .drop_index(&id, "idx_test_table_new", "test_table")
                .await
                .unwrap(),
            "success"
        );
    }

    #[tokio::test]
    async fn sql_validation_should_work() {
        let (_tdb, conn_str) = setup_test_db().await;
        let conns = Conns::new();
        let id = conns.register(conn_str).await.unwrap();

        let invalid_query = "INSERT INTO test_table VALUES (1)";
        assert!(conns.query(&id, invalid_query).await.is_err());

        let invalid_insert = "SELECT * FROM test_table";
        assert!(conns.insert(&id, invalid_insert).await.is_err());

        let invalid_update = "DELETE FROM test_table";
        assert!(conns.update(&id, invalid_update).await.is_err());

        let invalid_create = "CREATE INDEX idx_test ON test_table (id)";
        assert!(conns.create_table(&id, invalid_create).await.is_err());

        let invalid_index = "CREATE TABLE test (id INT)";
        assert!(conns.create_index(&id, invalid_index).await.is_err());
    }

    #[tokio::test]
    async fn create_schema_should_work() {
        let (_tdb, conn_str) = setup_test_db().await;
        let conns = Conns::new();
        let id = conns.register(conn_str).await.unwrap();

        let schema_name = "test_schema_unit";
        assert_eq!(
            conns.create_schema(&id, schema_name).await.unwrap(),
            "success"
        );

        let query = format!(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{}'",
            schema_name
        );
        let _result = sqlx::query(&query)
            .fetch_one(&conns.inner.load().get(&id).unwrap().pool)
            .await
            .unwrap();
    }
}
