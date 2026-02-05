use serde::{Deserialize, Serialize};
use stackable_operator::schemars::{self, JsonSchema};

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
