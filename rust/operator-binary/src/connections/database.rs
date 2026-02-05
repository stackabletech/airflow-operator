use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use stackable_operator::schemars::{self, JsonSchema};

#[derive(Clone, Deserialize, Debug, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DbType {
    #[serde(rename = "postgresql")]
    Postgresql(PostgresqlDb),
    #[serde(rename = "generic")]
    Generic(GenericDb),
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
pub struct GenericDb {
    pub uri_secret: String,
}

impl DbType {
    pub fn credentials_secret(&self) -> String {
        match self {
            DbType::Postgresql(db) => db.credentials_secret.to_owned(),
            DbType::Generic(db) => db.uri_secret.to_owned(),
        }
    }
}

impl PostgresqlDb {
    fn connection_string(&self, prefix: &str, username_env: &str, password_env: &str) -> String {
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
            "{}://${}:${}@{}:{}/{}{}",
            prefix, username_env, password_env, self.host, self.port, self.database_name, params
        )
    }

    pub fn connection_string_alchemy(&self, username_env: &str, password_env: &str) -> String {
        self.connection_string("postgresql+psycopg2", username_env, password_env)
    }

    pub fn connection_string_celery(&self, username_env: &str, password_env: &str) -> String {
        self.connection_string("db+postgresql", username_env, password_env)
    }
}

fn default_postgres_port() -> u16 {
    5432
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::connections::database::{PostgresqlDb, default_postgres_port};

    #[test]
    fn test_postgresql_alchemy() {
        let db_type = PostgresqlDb {
            host: "airflow-postgresql".to_string(),
            database_name: "airflow".to_string(),
            credentials_secret: "airflow-credentials".to_string(),
            port: default_postgres_port(),
            parameters: BTreeMap::new(),
        };
        let connection_string = db_type.connection_string_alchemy("DB_USERNAME", "DB_PASSWORD");

        assert_eq!(
            "postgresql+psycopg2://$DB_USERNAME:$DB_PASSWORD@airflow-postgresql:5432/airflow",
            connection_string
        );
    }
}
