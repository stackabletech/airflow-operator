use crate::airflow_controller::{CERTS_DIR, SECRETS_DIR};
use stackable_airflow_crd::{
    AirflowClusterAuthenticationConfig, AirflowConfigOptions, LdapRolesSyncMoment,
};
use stackable_operator::commons::{
    authentication::{AuthenticationClass, AuthenticationClassProvider},
    ldap::LdapAuthenticationProvider,
    tls::{CaCert, TlsVerification},
};
use std::collections::BTreeMap;

pub const PYTHON_IMPORTS: &[&str] = &[
    "import os",
    "from airflow.www.fab_security.manager import (AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER)",
    "basedir = os.path.abspath(os.path.dirname(__file__))",
    "WTF_CSRF_ENABLED = True",
];

pub fn add_airflow_config(
    config: &mut BTreeMap<String, String>,
    authentication_config: Option<&AirflowClusterAuthenticationConfig>,
    authentication_class: Option<&AuthenticationClass>,
) {
    if let Some(authentication_config) = authentication_config {
        if let Some(authentication_class) = authentication_class {
            append_authentication_config(config, authentication_config, authentication_class);
        }
    }
    if !config.contains_key(&*AirflowConfigOptions::AuthType.to_string()) {
        config.insert(
            // should default to AUTH_TYPE = AUTH_DB
            AirflowConfigOptions::AuthType.to_string(),
            "AUTH_DB".into(),
        );
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

#[cfg(test)]
mod tests {
    use crate::config::add_airflow_config;
    use crate::AirflowCluster;
    use stackable_airflow_crd::LdapRolesSyncMoment::Registration;
    use stackable_airflow_crd::{AirflowClusterAuthenticationConfig, AirflowConfigOptions};
    use stackable_operator::commons::authentication::AuthenticationClass;
    use std::collections::BTreeMap;

    #[test]
    fn test_no_ldap() {
        let cluster: AirflowCluster = serde_yaml::from_str::<AirflowCluster>(
            "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          version: 2.2.4
          executor: KubernetesExecutor
          loadExamples: true
          exposeConfig: true
          credentialsSecret: simple-airflow-credentials
          ",
        )
        .unwrap();

        let mut result = BTreeMap::new();
        add_airflow_config(
            &mut result,
            cluster.spec.authentication_config.as_ref(),
            None,
        );
        assert_eq!(None, cluster.spec.authentication_config.as_ref());
        assert_eq!(
            BTreeMap::from([("AUTH_TYPE".into(), "AUTH_DB".into())]),
            result
        );
    }

    #[test]
    fn test_ldap() {
        let cluster: AirflowCluster = serde_yaml::from_str::<AirflowCluster>(
            "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          version: 2.2.4
          executor: KubernetesExecutor
          loadExamples: true
          exposeConfig: true
          credentialsSecret: simple-airflow-credentials
          authenticationConfig:
            authenticationClass: airflow-with-ldap-server-veri-tls-ldap
            userRegistrationRole: Admin
          ",
        )
        .unwrap();

        let authentication_class: AuthenticationClass =
            serde_yaml::from_str::<AuthenticationClass>(
                "
            apiVersion: authentication.stackable.tech/v1alpha1
            kind: AuthenticationClass
            metadata:
              name: airflow-with-ldap-server-veri-tls-ldap
            spec:
              provider:
                ldap:
                  hostname: openldap.default.svc.cluster.local
                  port: 636
                  searchBase: ou=users,dc=example,dc=org
                  ldapFieldNames:
                    uid: uid
                  bindCredentials:
                    secretClass: airflow-with-ldap-server-veri-tls-ldap-bind
                  tls:
                    verification:
                      server:
                        caCert:
                          secretClass: openldap-tls
          ",
            )
            .unwrap();

        let mut result = BTreeMap::new();
        add_airflow_config(
            &mut result,
            cluster.spec.authentication_config.as_ref(),
            Some(&authentication_class),
        );
        assert_eq!(
            Some(AirflowClusterAuthenticationConfig {
                authentication_class: Some("airflow-with-ldap-server-veri-tls-ldap".to_string()),
                user_registration: true,
                user_registration_role: "Admin".to_string(),
                sync_roles_at: Registration
            }),
            cluster.spec.authentication_config
        );
        assert_eq!(
            "AUTH_LDAP",
            result
                .get(&AirflowConfigOptions::AuthType.to_string())
                .unwrap()
        );
        println!("{:#?}", result);
    }
}
