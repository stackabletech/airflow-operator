use std::{ops::Deref, sync::Arc};

use clap::Parser;
use futures::StreamExt;
use stackable_operator::{
    YamlSchema,
    cli::{Command, ProductOperatorRun, RollingPeriod},
    commons::authentication::AuthenticationClass,
    k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service},
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
};
use stackable_telemetry::{Tracing, tracing::settings::Settings};
use tracing::level_filters::LevelFilter;

use crate::{
    airflow_controller::AIRFLOW_FULL_CONTROLLER_NAME,
    crd::{AirflowCluster, OPERATOR_NAME, v1alpha1},
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
            telemetry_arguments,
            cluster_info_opts,
        }) => {
            let _tracing_guard = Tracing::builder()
                // TODO (@Techassi): This should be a constant
                .service_name("airflow-operator")
                .with_console_output((
                    // TODO (@Techassi): Change to CONSOLE_LOG, create constant
                    "AIRFLOW_OPERATOR_LOG",
                    LevelFilter::INFO,
                    !telemetry_arguments.no_console_output,
                ))
                // note, before, log dir was set via an env: `AIRFLOW_OPERATOR_LOG_DIRECTORY`.
                // See: https://github.com/stackabletech/operator-rs/blob/f035997fca85a54238c8de895389cc50b4d421e2/crates/stackable-operator/src/logging/mod.rs#L40
                // Now it will be `ROLLING_LOGS` (or via `--rolling-logs <DIRECTORY>`)
                .with_file_output(telemetry_arguments.rolling_logs.map(|log_directory| {
                    let rotation_period = telemetry_arguments
                        .rolling_logs_period
                        .unwrap_or(RollingPeriod::Hourly)
                        .deref()
                        .clone();

                    Settings::builder()
                        // TODO (@Techassi): Change to CONSOLE_LOG or FILE_LOG, create constant
                        .with_environment_variable("AIRFLOW_OPERATOR_LOG")
                        .with_default_level(LevelFilter::INFO)
                        .file_log_settings_builder(log_directory, "tracing-rs.log")
                        .with_rotation_period(rotation_period)
                        .build()
                }))
                .with_otlp_log_exporter((
                    "OTLP_LOG",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_logs,
                ))
                .with_otlp_trace_exporter((
                    "OTLP_TRACE",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_traces,
                ))
                .build()
                .init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
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

            let event_recorder = Arc::new(Recorder::new(client.as_kube_client(), Reporter {
                controller: AIRFLOW_FULL_CONTROLLER_NAME.to_string(),
                instance: None,
            }));

            let airflow_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::AirflowCluster>>(&client),
                watcher::Config::default(),
            );

            let airflow_store_1 = airflow_controller.store();
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
