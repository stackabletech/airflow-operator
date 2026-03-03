use serde::{Deserialize, Serialize};
use stackable_operator::{
    databases::{
        databases::{postgresql::PostgresqlConnection, redis::RedisConnection},
        drivers::{
            celery::{CeleryDatabaseConnection, GenericCeleryDatabaseConnection},
            sqlalchemy::{GenericSQLAlchemyDatabaseConnection, SQLAlchemyDatabaseConnection},
        },
    },
    schemars::{self, JsonSchema},
};

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum MetadataDatabaseConnection {
    /// TODO docs
    Postgresql(PostgresqlConnection),

    /// TODO docs
    Generic(GenericSQLAlchemyDatabaseConnection),
}

impl MetadataDatabaseConnection {
    pub fn as_sqlalchemy_database_connection(&self) -> &dyn SQLAlchemyDatabaseConnection {
        match self {
            Self::Postgresql(p) => p,
            Self::Generic(g) => g,
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CeleryResultBackendConnection {
    /// TODO docs
    Postgresql(PostgresqlConnection),

    /// TODO docs
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
    /// TODO docs
    Redis(RedisConnection),

    /// TODO docs
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
