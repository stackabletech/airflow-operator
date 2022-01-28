mod airflow_controller;
mod init_controller;

use clap::Parser;
use futures::StreamExt;
use stackable_airflow_crd::{commands::Init, AirflowCluster};
use stackable_operator::cli::ProductOperatorRun;
use stackable_operator::{
    cli::Command,
    k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service},
    kube::{
        api::{DynamicObject, ListParams},
        runtime::{
            controller::{Context, ReconcilerAction},
            reflector::ObjectRef,
            Controller,
        },
        CustomResourceExt, Resource,
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

/// Erases the concrete types of the controller result, so that we can merge the streams of multiple controllers for different resources.
///
/// In particular, we convert `ObjectRef<K>` into `ObjectRef<DynamicObject>` (which carries `K`'s metadata at runtime instead), and
/// `E` into the trait object `anyhow::Error`.
fn erase_controller_result_type<K: Resource, E: std::error::Error + Send + Sync + 'static>(
    res: Result<(ObjectRef<K>, ReconcilerAction), E>,
) -> anyhow::Result<(ObjectRef<DynamicObject>, ReconcilerAction)> {
    let (obj_ref, action) = res?;
    Ok((obj_ref.erase(), action))
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
        Command::Run(ProductOperatorRun { product_config }) => {
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
                client.get_all_api::<AirflowCluster>(),
                ListParams::default(),
            )
            .shutdown_on_signal()
            .owns(client.get_all_api::<Service>(), ListParams::default())
            .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
            .run(
                airflow_controller::reconcile_airflow,
                airflow_controller::error_policy,
                Context::new(airflow_controller::Ctx {
                    client: client.clone(),
                    product_config,
                }),
            );

            let init_controller =
                Controller::new(client.get_all_api::<Init>(), ListParams::default())
                    .shutdown_on_signal()
                    .run(
                        init_controller::reconcile_init,
                        init_controller::error_policy,
                        Context::new(init_controller::Ctx {
                            client: client.clone(),
                        }),
                    );

            futures::stream::select(
                airflow_controller.map(erase_controller_result_type),
                init_controller.map(erase_controller_result_type),
            )
            .for_each(|res| async {
                match res {
                    Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                    Err(err) => {
                        tracing::error!(
                            error = &*err as &dyn std::error::Error,
                            "Failed to reconcile object",
                        )
                    }
                }
            })
            .await;
        }
    }

    Ok(())
}
