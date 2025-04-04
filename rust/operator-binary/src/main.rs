use std::sync::Arc;

use clap::{crate_description, crate_version, Parser};
use futures::StreamExt;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    commons::authentication::AuthenticationClass,
    k8s_openapi::api::{apps::v1::StatefulSet, core::v1::ConfigMap, core::v1::Service},
    kube::{
        core::DeserializeGuard,
        runtime::{
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher, Controller,
        },
        ResourceExt,
    },
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
    YamlSchema,
};

use crate::{
    airflow_controller::AIRFLOW_FULL_CONTROLLER_NAME,
    crd::{v1alpha1, AirflowCluster, APP_NAME, OPERATOR_NAME},
};

mod airflow_controller;
mod config;
mod controller_commons;
mod crd;
mod env_vars;
mod operations;
mod product_logging;
mod util;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

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
            AirflowCluster::merged_crd(AirflowCluster::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
        }
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
            cluster_info_opts,
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
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/airflow-operator/config-spec/properties.yaml",
            ])?;

            let client = stackable_operator::client::initialize_operator(
                Some(OPERATOR_NAME.to_string()),
                &cluster_info_opts,
            )
            .await?;

            let event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: AIRFLOW_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));

            let airflow_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::AirflowCluster>>(&client),
                watcher::Config::default(),
            );

            let airflow_store_1 = airflow_controller.store();
            let airflow_store_2 = airflow_controller.store();
            airflow_controller
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
                    client.get_api::<DeserializeGuard<AuthenticationClass>>(&()),
                    watcher::Config::default(),
                    move |authentication_class| {
                        airflow_store_1
                            .state()
                            .into_iter()
                            .filter(
                                move |airflow: &Arc<DeserializeGuard<v1alpha1::AirflowCluster>>| {
                                    references_authentication_class(airflow, &authentication_class)
                                },
                            )
                            .map(|airflow| ObjectRef::from_obj(&*airflow))
                    },
                )
                .watches(
                    watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                    watcher::Config::default(),
                    move |config_map| {
                        airflow_store_2
                            .state()
                            .into_iter()
                            .filter(move |airflow| references_config_map(airflow, &config_map))
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
                // We can let the reporting happen in the background
                .for_each_concurrent(
                    16, // concurrency limit
                    |result| {
                        // The event_recorder needs to be shared across all invocations, so that
                        // events are correctly aggregated
                        let event_recorder = event_recorder.clone();
                        async move {
                            report_controller_reconciled(
                                &event_recorder,
                                AIRFLOW_FULL_CONTROLLER_NAME,
                                &result,
                            )
                            .await;
                        }
                    },
                )
                .await;
        }
    }

    Ok(())
}

fn references_authentication_class(
    airflow: &DeserializeGuard<v1alpha1::AirflowCluster>,
    authentication_class: &DeserializeGuard<AuthenticationClass>,
) -> bool {
    let Ok(airflow) = &airflow.0 else {
        return false;
    };
    let authentication_class_name = authentication_class.name_any();

    airflow
        .spec
        .cluster_config
        .authentication
        .iter()
        .any(|c| c.common.authentication_class_name() == &authentication_class_name)
}

fn references_config_map(
    airflow: &DeserializeGuard<v1alpha1::AirflowCluster>,
    config_map: &DeserializeGuard<ConfigMap>,
) -> bool {
    let Ok(airflow) = &airflow.0 else {
        return false;
    };
    // Check for ConfigMaps that are referenced by the spec and not directly attached to a Pod
    match airflow.spec.cluster_config.authorization.clone() {
        Some(airflow_authorization) => match airflow_authorization.opa {
            Some(opa_config) => opa_config.opa.config_map_name == config_map.name_any(),
            None => false,
        },
        None => false,
    }
}
