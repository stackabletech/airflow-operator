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

/// Adds a CA file from `cert_file` to the root certificates of Python Certifi

pub fn add_cert_to_python_certifi_command(cert_file: &str) -> String {
    format!("cat {cert_file} >> \"$(python -c 'import certifi; print(certifi.where())')\"")
}
