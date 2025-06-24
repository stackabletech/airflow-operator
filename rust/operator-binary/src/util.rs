use stackable_operator::k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, SecretKeySelector};

pub fn env_var_from_secret(var_name: &str, secret: &str, secret_key: &str) -> EnvVar {
    EnvVar {
        name: String::from(var_name),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: String::from(secret),
                key: String::from(secret_key),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn role_service_name(name: &str, role: &str) -> String {
    format!("{name}-{role}")
}
