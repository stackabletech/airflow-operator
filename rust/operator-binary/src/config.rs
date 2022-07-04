use crate::airflow_controller::{CERTS_DIR, SECRETS_DIR};
use stackable_operator::commons::{
    authentication::{AuthenticationClass, AuthenticationClassProvider},
    ldap::LdapAuthenticationProvider,
    tls::{CaCert, TlsVerification},
};
use stackable_airflow_crd::{
    LdapRolesSyncMoment, AirflowClusterAuthenticationConfig, AirflowConfigOptions,
};
use std::collections::BTreeMap;

pub const PYTHON_IMPORTS: &[&str] = &[
    "import os",
    "from airflow.stats_logger import StatsdStatsLogger",
    "from flask_appbuilder.security.manager import (AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER)",
    ];

pub fn add_airflow_config(
    config: &mut BTreeMap<String, String>,
    authentication_config: Option<&AirflowClusterAuthenticationConfig>,
    authentication_class: Option<&AuthenticationClass>,
) {
    config.insert(
        AirflowConfigOptions::SecretKey.to_string(),
        "os.environ.get('SECRET_KEY')".into(),
    );
    config.insert(
        AirflowConfigOptions::SqlalchemyDatabaseUri.to_string(),
        "os.environ.get('SQLALCHEMY_DATABASE_URI')".into(),
    );
    config.insert(
        AirflowConfigOptions::StatsLogger.to_string(),
        "StatsdStatsLogger(host='0.0.0.0', port=9125)".into(),
    );

    if let Some(authentication_config) = authentication_config {
        if let Some(authentication_class) = authentication_class {
            append_authentication_config(config, authentication_config, authentication_class);
        }
    }
}

fn append_authentication_config(
    config: &mut BTreeMap<String, String>,
    authentication_config: &AirflowClusterAuthenticationConfig,
    authentication_class: &AuthenticationClass,
) {
    let authentication_class_name = authentication_class.metadata.name.as_ref().unwrap();
    if let AuthenticationClassProvider::Ldap(ldap) = &authentication_class.spec.provider {
        append_ldap_config(config, ldap, authentication_class_name);
    }

    config.insert(
        AirflowConfigOptions::AuthUserRegistration.to_string(),
        authentication_config.user_registration.to_string(),
    );
    config.insert(
        AirflowConfigOptions::AuthUserRegistrationRole.to_string(),
        authentication_config.user_registration_role.to_string(),
    );
    config.insert(
        AirflowConfigOptions::AuthRolesSyncAtLogin.to_string(),
        (authentication_config.sync_roles_at == LdapRolesSyncMoment::Login).to_string(),
    );
}

fn append_ldap_config(
    config: &mut BTreeMap<String, String>,
    ldap: &LdapAuthenticationProvider,
    authentication_class_name: &str,
) {
    config.insert(
        AirflowConfigOptions::AuthType.to_string(),
        "AUTH_LDAP".into(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapServer.to_string(),
        format!(
            "{protocol}{server_hostname}:{server_port}",
            protocol = match ldap.tls {
                None => "ldap://",
                Some(_) => "ldaps://",
            },
            server_hostname = ldap.hostname,
            server_port = ldap.port.unwrap_or_else(|| ldap.default_port()),
        ),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapSearch.to_string(),
        ldap.search_base.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapSearchFilter.to_string(),
        ldap.search_filter.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapUidField.to_string(),
        ldap.ldap_field_names.uid.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapGroupField.to_string(),
        ldap.ldap_field_names.group.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapFirstnameField.to_string(),
        ldap.ldap_field_names.given_name.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapLastnameField.to_string(),
        ldap.ldap_field_names.surname.clone(),
    );

    // Possible TLS options, see https://github.com/dpgaspar/Flask-AppBuilder/blob/f6f66fc1bcc0163a213e4a2e6f960e91082d201f/flask_appbuilder/security/manager.py#L243-L250
    match &ldap.tls {
        None => {
            config.insert(
                AirflowConfigOptions::AuthLdapTlsDemand.to_string(),
                false.to_string(),
            );
        }
        Some(tls) => match &tls.verification {
            TlsVerification::None {} => {
                config.insert(
                    AirflowConfigOptions::AuthLdapTlsDemand.to_string(),
                    true.to_string(),
                );
                config.insert(
                    AirflowConfigOptions::AuthLdapAllowSelfSigned.to_string(),
                    true.to_string(),
                );
            }
            TlsVerification::Server(server_verification) => {
                append_server_ca_cert(
                    config,
                    authentication_class_name,
                    &server_verification.ca_cert,
                );
            }
        },
    }

    if ldap.bind_credentials.is_some() {
        config.insert(
            AirflowConfigOptions::AuthLdapBindUser.to_string(),
            format!(
                "open('{SECRETS_DIR}{authentication_class_name}-bind-credentials/user').read()"
            ),
        );
        config.insert(
            AirflowConfigOptions::AuthLdapBindPassword.to_string(),
            format!(
                "open('{SECRETS_DIR}{authentication_class_name}-bind-credentials/password').read()"
            ),
        );
    }
}

fn append_server_ca_cert(
    config: &mut BTreeMap<String, String>,
    authentication_class_name: &str,
    server_ca_cert: &CaCert,
) {
    config.insert(
        AirflowConfigOptions::AuthLdapTlsDemand.to_string(),
        true.to_string(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapAllowSelfSigned.to_string(),
        false.to_string(),
    );
    match server_ca_cert {
        CaCert::SecretClass(..) => {
            config.insert(
                AirflowConfigOptions::AuthLdapTlsCacertfile.to_string(),
                format!("{CERTS_DIR}{authentication_class_name}-tls-certificate/ca.crt"),
            );
        }
        CaCert::WebPki {} => {}
    }
}