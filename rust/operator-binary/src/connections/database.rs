use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use stackable_operator::schemars::{self, JsonSchema};

// Struct defining supported backend database types.
// The implementation should conform to ADR 29.
// Concrete types will define all fields necessary to construct a connection for that database type.
// Each backend requires a secret containing `username` and `password`.
// Additionally, a generic type is defined which will contain a single field pointing to a secret with a single `uri` field that contains the entire connection string.
// A single backend type may use multiple drivers that are prefixed to the connection string.
// The operator knows the context of the backend connection (e.g. is it for a SqlALchemyURI- or a Celery-connection) and is therefore responsible for calling the correct function to obtain the connnection string.
// The connection string will not contain resolved credentials, but rather embedded environment variables that point to values that have been set via the resource definitions (created by the operator).
// In this way the operator never has to read the secret itself.
// These embedded values within enviroment variables are resolved when read by product containers, as they are read through a process started within a shell (which performs variable substitution).
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
    // At this point the necessary secrets have been added to the product
    // statefulset (as env-vars). If a product has been started via a shell,
    // then any embedded environment variables will be resolved (i.e. variable
    // substitution) automatically. If for some reason the env-var is accessed
    // directly via program code (e.g. python), then this will not work, and
    // the env-vars will instead need to be replaced with the same information
    // written to a relevant configuration file (as env-vars cannot be safely
    // templated/re-written from Rust in a multi-threaded environment): in this
    // case the resulting connection will be written (where env-vars are in the
    // form of ${{env:...}}) to a file which is then templated on container
    // start-up.
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
