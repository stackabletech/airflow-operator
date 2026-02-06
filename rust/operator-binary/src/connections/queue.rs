use serde::{Deserialize, Serialize};
use stackable_operator::schemars::{self, JsonSchema};

// Struct defining supported backend queue/broker types.
// These are similar to backend databases but are kept separate as they are not strictly covered by ADR 29.
// Concrete types will define all fields necessary to construct a connection for that queue type.
// Each queue requires a secret containing `username` and `password`.
// Additionally, a generic type is defined which will contain a single field pointing to a secret with a single `uri` field that contains the entire connection string.
// A single queue type may use multiple drivers that are prefixed to the connection string.
// The operator knows the context of the queue connection and is therefore responsible for calling the correct function.
// The connection string will not contain resolved credentials, but rather embedded environment variables that point to values that have been set via the resource definitions (created by the operator).
// In this way the operator never has to read the secret itself.
// These embedded values within enviroment variables are resolved when read by product containers, as they are read through a process started within a shell (which performs variable substitution).
#[derive(Clone, Deserialize, Debug, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum QueueType {
    #[serde(rename = "redis")]
    Redis(RedisQueue),
    #[serde(rename = "generic")]
    Generic(GenericQueue),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RedisQueue {
    pub host: String,
    #[serde(default = "default_redis_port")]
    pub port: u16,
    pub credentials_secret: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenericQueue {
    pub uri_secret: String,
}

impl QueueType {
    pub fn credentials_secret(&self) -> String {
        match self {
            QueueType::Redis(queue) => queue.credentials_secret.to_owned(),
            QueueType::Generic(queue) => queue.uri_secret.to_owned(),
        }
    }
}

impl RedisQueue {
    pub fn connection_string(&self, username_env: &str, password_env: &str) -> String {
        format!(
            "redis://${}:${}@{}:{}/0",
            username_env, password_env, self.host, self.port
        )
    }
}

fn default_redis_port() -> u16 {
    6379
}

#[cfg(test)]
mod tests {
    use crate::connections::queue::{RedisQueue, default_redis_port};

    #[test]
    fn test_redis_queue() {
        let queue_type = RedisQueue {
            host: "airflow-postgresql".to_string(),
            credentials_secret: "airflow-credentials".to_string(),
            port: default_redis_port(),
        };
        let connection_string = queue_type.connection_string("QUEUE_USERNAME", "QUEUE_PASSWORD");

        assert_eq!(
            "redis://$QUEUE_USERNAME:$QUEUE_PASSWORD@airflow-postgresql:6379/0",
            connection_string
        );
    }
}
