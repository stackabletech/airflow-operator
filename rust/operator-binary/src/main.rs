mod airflow_controller;
mod init_controller;

use clap::Parser;
use futures::StreamExt;
use stackable_airflow_crd::{commands::Init, AirflowCluster};
use stackable_operator::cli::ProductOperatorRun;
use stackable_operator::logging::controller::report_controller_reconciled;
use stackable_operator::{
    cli::Command,
    k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service},
    kube::{
        api::ListParams,
        runtime::{controller::Context, Controller},
        CustomResourceExt,
    },
};

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stackable_operator::logging::initialize_logging("AIRFLOW_OPERATOR_LOG");

    let opts = Opts::parse();

    match opts.cmd {
        Command::Crd => println!(
            "{}{}",
            serde_yaml::to_string(&AirflowCluster::crd())?,
            serde_yaml::to_string(&Init::crd())?
        ),
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
        }) => {
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/airflow-operator/config-spec/properties.yaml",
            ])?;
            let client = stackable_operator::client::create_client(Some(
                "airflow.stackable.tech".to_string(),
            ))
            .await?;
            let airflow_controller = Controller::new(
                watch_namespace.get_api::<AirflowCluster>(&client),
                ListParams::default(),
            )
            .shutdown_on_signal()
            .owns(
                watch_namespace.get_api::<Service>(&client),
                ListParams::default(),
            )
            .owns(
                watch_namespace.get_api::<StatefulSet>(&client),
                ListParams::default(),
            )
            .run(
                airflow_controller::reconcile_airflow,
                airflow_controller::error_policy,
                Context::new(airflow_controller::Ctx {
                    client: client.clone(),
                    product_config,
                }),
            )
            .map(|res| {
                report_controller_reconciled(
                    &client,
                    "airflowclusters.airflow.stackable.tech",
                    &res,
                );
            });

            let init_controller = Controller::new(
                watch_namespace.get_api::<Init>(&client),
                ListParams::default(),
            )
            .shutdown_on_signal()
            .run(
                init_controller::reconcile_init,
                init_controller::error_policy,
                Context::new(init_controller::Ctx {
                    client: client.clone(),
                }),
            )
            .map(|res| {
                report_controller_reconciled(&client, "inits.command.airflow.stackable.tech", &res);
            });

            futures::stream::select(airflow_controller, init_controller)
                .collect::<()>()
                .await;
        }
    }

    Ok(())
}
