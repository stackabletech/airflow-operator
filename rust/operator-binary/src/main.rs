// TODO: Look into how to properly resolve `clippy::result_large_err`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use anyhow::anyhow;
use clap::Parser;
use futures::{FutureExt, StreamExt, TryFutureExt};
use stackable_operator::{
    YamlSchema,
    cli::{Command, RunArguments},
    crd::authentication::core as auth_core,
    eos::EndOfSupportChecker,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{
        ResourceExt,
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
    telemetry::Tracing,
};

use crate::{
    airflow_controller::AIRFLOW_FULL_CONTROLLER_NAME,
    crd::{AirflowCluster, AirflowClusterVersion, OPERATOR_NAME, v1alpha2},
    webhooks::conversion::create_webhook_server,
};

mod airflow_controller;
mod config;
mod connection;
mod connections;
mod controller_commons;
mod crd;
mod env_vars;
mod operations;
mod product_logging;
mod service;
mod util;
mod webhooks;

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
            AirflowCluster::merged_crd(AirflowClusterVersion::V1Alpha2)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
        }
        Command::Run(RunArguments {
            product_config,
            watch_namespace,
            operator_environment,
            maintenance,
            common,
        }) => {
            // NOTE (@NickLarsenNZ): Before stackable-telemetry was used:
            // - The console log level was set by `AIRFLOW_OPERATOR_LOG`, and is now `CONSOLE_LOG` (when using Tracing::pre_configured).
            // - The file log level was set by `AIRFLOW_OPERATOR_LOG`, and is now set via `FILE_LOG` (when using Tracing::pre_configured).
            // - The file log directory was set by `AIRFLOW_OPERATOR_LOG_DIRECTORY`, and is now set by `ROLLING_LOGS_DIR` (or via `--rolling-logs <DIRECTORY>`).
            let _tracing_guard =
                Tracing::pre_configured(built_info::PKG_NAME, common.telemetry).init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
            );

            let eos_checker =
                EndOfSupportChecker::new(built_info::BUILT_TIME_UTC, maintenance.end_of_support)?
                    .run()
                    .map(anyhow::Ok);

            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/airflow-operator/config-spec/properties.yaml",
            ])?;

            let client = stackable_operator::client::initialize_operator(
                Some(OPERATOR_NAME.to_string()),
                &common.cluster_info,
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
                watch_namespace.get_api::<DeserializeGuard<v1alpha2::AirflowCluster>>(&client),
                watcher::Config::default(),
            );

            let authentication_class_store = airflow_controller.store();
            let config_map_store = airflow_controller.store();
            let airflow_controller = airflow_controller
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
                    client
                        .get_api::<DeserializeGuard<auth_core::v1alpha1::AuthenticationClass>>(&()),
                    watcher::Config::default(),
                    move |authentication_class| {
                        authentication_class_store
                            .state()
                            .into_iter()
                            .filter(
                                move |airflow: &Arc<DeserializeGuard<v1alpha2::AirflowCluster>>| {
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
                        config_map_store
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
                .map(anyhow::Ok);

            let webhook_server = create_webhook_server(
                &operator_environment,
                maintenance.disable_crd_maintenance,
                client.as_kube_client(),
            )
            .await?;

            let webhook_server = webhook_server
                .run()
                .map_err(|err| anyhow!(err).context("failed to run webhook server"));

            futures::try_join!(airflow_controller, webhook_server, eos_checker)?;
        }
    }

    Ok(())
}

fn references_authentication_class(
    airflow: &DeserializeGuard<v1alpha2::AirflowCluster>,
    authentication_class: &DeserializeGuard<auth_core::v1alpha1::AuthenticationClass>,
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
    airflow: &DeserializeGuard<v1alpha2::AirflowCluster>,
    config_map: &DeserializeGuard<ConfigMap>,
) -> bool {
    let Ok(airflow) = &airflow.0 else {
        return false;
    };
    // Check for ConfigMaps that are referenced by the spec and not directly attached to a Pod
    match &airflow.spec.cluster_config.authorization {
        Some(airflow_authorization) => match &airflow_authorization.opa {
            Some(opa_config) => opa_config.opa.config_map_name == config_map.name_any(),
            None => false,
        },
        None => false,
    }
}
