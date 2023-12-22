mod airflow_controller;
mod config;
mod controller_commons;
mod operations;
mod product_logging;
mod util;

use crate::airflow_controller::AIRFLOW_CONTROLLER_NAME;
use semver::Version;
use std::collections::BTreeMap;
use std::io::Write;
use std::string::ToString;

use clap::{crate_description, crate_version, Parser};
use futures::StreamExt;
use stackable_airflow_crd::{
    authentication::AirflowAuthentication, AirflowCluster, APP_NAME, OPERATOR_NAME,
};
use stackable_operator::error::{Error, OperatorResult};
use stackable_operator::k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceConversion, ServiceReference, WebhookClientConfig, WebhookConversion,
};

use stackable_operator::kube::core::crd::merge_crds;
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    commons::authentication::AuthenticationClass,
    k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service},
    kube::{
        runtime::{reflector::ObjectRef, watcher, Controller},
        ResourceExt,
    },
    logging::controller::report_controller_reconciled,
};
use std::sync::Arc;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
    pub const TARGET_PLATFORM: Option<&str> = option_env!("TARGET");
    pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
}

const LATEST_API_VERSION: &str = "v1beta1";
const INJECTOR_ANNOTATION_KEY: &str = "cert-manager.io/inject-apiserver-ca";

#[derive(Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    match opts.cmd {
        Command::Crd => {
            print_multi_version_yaml_schema(built_info::CARGO_PKG_VERSION)?;
        }
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
        }) => {
            stackable_operator::logging::initialize_logging(
                "AIRFLOW_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );
            stackable_operator::utils::print_startup_string(
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET_PLATFORM.unwrap_or("unknown target"),
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/airflow-operator/config-spec/properties.yaml",
            ])?;

            let client =
                stackable_operator::client::create_client(Some(OPERATOR_NAME.to_string())).await?;

            let airflow_controller_builder = Controller::new(
                watch_namespace.get_api::<AirflowCluster>(&client),
                watcher::Config::default(),
            );

            let airflow_store_1 = airflow_controller_builder.store();
            let airflow_controller = airflow_controller_builder
                .owns(
                    watch_namespace.get_api::<Service>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<StatefulSet>(&client),
                    watcher::Config::default(),
                )
                .shutdown_on_signal()
                .watches(
                    client.get_api::<AuthenticationClass>(&()),
                    watcher::Config::default(),
                    move |authentication_class| {
                        airflow_store_1
                            .state()
                            .into_iter()
                            .filter(move |airflow: &Arc<AirflowCluster>| {
                                references_authentication_class(
                                    &airflow.spec.cluster_config.authentication,
                                    &authentication_class,
                                )
                            })
                            .map(|airflow| ObjectRef::from_obj(&*airflow))
                    },
                )
                .run(
                    airflow_controller::reconcile_airflow,
                    airflow_controller::error_policy,
                    Arc::new(airflow_controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                )
                .map(|res| {
                    report_controller_reconciled(
                        &client,
                        &format!("{AIRFLOW_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                        &res,
                    );
                });

            airflow_controller.collect::<()>().await;
        }
    }

    Ok(())
}

fn print_multi_version_yaml_schema(operator_version: &str) -> OperatorResult<()> {
    let mut writer = std::io::stdout();
    let crd_alpha = stackable_airflow_crd::v1alpha1::lib::AirflowCluster::crd();
    let crd_beta = AirflowCluster::crd();
    let mut crd_composite = merge_crds(
        vec![crd_alpha.clone(), crd_beta.clone()],
        LATEST_API_VERSION,
    )
    .unwrap(); // TODO add error handling when this function is added to the framework

    crd_composite.spec.conversion = Some(CustomResourceConversion {
        strategy: "Webhook".to_string(),
        webhook: Some(WebhookConversion {
            client_config: Some(WebhookClientConfig {
                ca_bundle: None,
                service: Some(ServiceReference {
                    name: "conversion-webhook-server".to_string(),
                    namespace: "default".to_string(),
                    path: Some("/convert".to_string()),
                    port: None, // TODO
                }),
                url: None,
            }),
            conversion_review_versions: vec!["v1".to_string()],
        }),
    });
    crd_composite
        .metadata
        .annotations
        .get_or_insert_with(BTreeMap::new)
        // if the cert-manager components are installed, this will populate the caBundle
        // field with the CA certificate used by the Kubernetes API server
        .insert(INJECTOR_ANNOTATION_KEY.to_string(), "true".to_string());

    let yaml = serde_yaml::to_string(&crd_composite)?.replace(
        "DOCS_BASE_URL_PLACEHOLDER",
        &docs_home_versioned_base_url(operator_version)?,
    );

    Ok(writer.write_all(yaml.as_bytes())?)
}

// TODO remove when merge function is moved to framework
fn docs_home_versioned_base_url(operator_version: &str) -> OperatorResult<String> {
    Ok(format!(
        "{}/{}",
        "https://docs.stackable.tech/home",
        docs_version(operator_version)?
    ))
}

// TODO remove when merge function is moved to framework
fn docs_version(operator_version: &str) -> OperatorResult<String> {
    if operator_version == "0.0.0-dev" {
        Ok("nightly".to_owned())
    } else {
        let v = Version::parse(operator_version).map_err(|err| Error::InvalidSemverVersion {
            source: err,
            version: operator_version.to_owned(),
        })?;
        Ok(format!("{}.{}", v.major, v.minor))
    }
}

fn references_authentication_class(
    authentication_config: &AirflowAuthentication,
    authentication_class: &AuthenticationClass,
) -> bool {
    assert!(authentication_class.metadata.name.is_some());

    authentication_config
        .authentication_class_names()
        .into_iter()
        .filter(|c| *c == authentication_class.name_any())
        .count()
        > 0
}
