use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use stackable_operator::schemars::{self, JsonSchema};

#[derive(Clone, Deserialize, Debug, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DbType {
    #[serde(rename = "postgresql")]
    Postgresql(PostgresqlDb),
    #[serde(rename = "redis")]
    Redis(RedisDb),
    #[serde(rename = "generic")]
    Generic(GenericDb),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenericDb {
    pub uri_secret: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PostgresqlDb {
    pub host: String,
    #[serde(default = "default_postgres_port")]
    pub port: u16,
    pub database_name: String,
    pub credentials_secret: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisDb {
    pub host: String,
    #[serde(default = "default_redis_port")]
    pub port: u16,
    pub credentials_secret: String,
}

impl DbType {
    pub fn connection_string(&self, username_env: &str, password_env: &str) -> String {
        match self {
            DbType::Postgresql(db) => db.connection_string(username_env, password_env),
            DbType::Redis(db) => db.connection_string(username_env, password_env),
            DbType::Generic(db) => db.connection_string(),
        }
    }
}

impl PostgresqlDb {
    pub fn connection_string(&self, username_env: &str, password_env: &str) -> String {
        let params = if self.parameters.is_empty() {
            String::new()
        } else {
            let param_str: Vec<String> = self
                .parameters
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            format!("?{}", param_str.join("&"))
        };

        format!(
            "postgresql+psycopg2://${{env:{}}}:${{env:{}}}@{}:{}/{}{}",
            username_env, password_env, self.host, self.port, self.database_name, params
        )
    }
}

impl RedisDb {
    pub fn connection_string(&self, username_env: &str, password_env: &str) -> String {
        format!(
            "redis://${{env:{}}}:${{env:{}}}@{}:{}/0",
            username_env, password_env, self.host, self.port
        )
    }
}

impl GenericDb {
    pub fn connection_string(&self) -> String {
        format!("${{secret:{}}}", self.uri_secret)
    }
}

fn default_postgres_port() -> u16 {
    5432
}

fn default_redis_port() -> u16 {
    6379
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::connections::database::{
        DbType, PostgresqlDb, RedisDb, default_postgres_port, default_redis_port,
    };

    #[test]
    fn test_postgresql_connection() {
        let db_type = DbType::Postgresql(PostgresqlDb {
            host: "airflow-postgresql".to_string(),
            database_name: "airflow".to_string(),
            credentials_secret: "airflow-credentials".to_string(),
            port: default_postgres_port(),
            parameters: BTreeMap::new(),
        });
        let connection_string = db_type.connection_string("ADMIN_USERNAME", "ADMIN_PASSWORD");

        assert_eq!(
            "postgresql+psycopg2://${env:ADMIN_USERNAME}:${env:ADMIN_PASSWORD}@airflow-postgresql:5432/airflow",
            connection_string
        );
    }

    #[test]
    fn test_redis_connection() {
        let db_type = DbType::Redis(RedisDb {
            host: "airflow-postgresql".to_string(),
            credentials_secret: "airflow-credentials".to_string(),
            port: default_redis_port(),
        });
        let connection_string = db_type.connection_string("ADMIN_USERNAME", "ADMIN_PASSWORD");

        assert_eq!(
            "redis://${env:ADMIN_USERNAME}:${env:ADMIN_PASSWORD}@airflow-postgresql:6379/0",
            connection_string
        );
    }
}
