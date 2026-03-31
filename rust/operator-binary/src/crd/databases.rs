use serde::{Deserialize, Serialize};
use stackable_operator::{
    database_connections::{
        databases::{postgresql::PostgresqlConnection, redis::RedisConnection},
        drivers::{
            celery::{CeleryDatabaseConnection, GenericCeleryDatabaseConnection},
            sqlalchemy::{GenericSqlAlchemyDatabaseConnection, SqlAlchemyDatabaseConnection},
        },
    },
    schemars::{self, JsonSchema},
};

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum MetadataDatabaseConnection {
    // Docs are on the struct
    Postgresql(PostgresqlConnection),

    // Docs are on the struct
    Generic(GenericSqlAlchemyDatabaseConnection),
}

impl MetadataDatabaseConnection {
    pub fn as_sqlalchemy_database_connection(&self) -> &dyn SqlAlchemyDatabaseConnection {
        match self {
            Self::Postgresql(p) => p,
            Self::Generic(g) => g,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CeleryResultBackendConnection {
    // Docs are on the struct
    Postgresql(PostgresqlConnection),

    // Docs are on the struct
    Generic(GenericCeleryDatabaseConnection),
}

impl CeleryResultBackendConnection {
    pub fn as_celery_database_connection(&self) -> &dyn CeleryDatabaseConnection {
        match self {
            Self::Postgresql(p) => p,
            Self::Generic(g) => g,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CeleryBrokerConnection {
    // Docs are on the struct
    Redis(RedisConnection),

    // Docs are on the struct
    Generic(GenericCeleryDatabaseConnection),
}

impl CeleryBrokerConnection {
    pub fn as_celery_database_connection(&self) -> &dyn CeleryDatabaseConnection {
        match self {
            Self::Redis(r) => r,
            Self::Generic(g) => g,
        }
    }
}
