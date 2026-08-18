use stackable_operator::{
    crd::listener,
    v2::types::kubernetes::{ListenerClassName, ListenerName},
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{object_meta, recommended_labels_for_role_resources},
    },
    crd::{AirflowRole, HTTP_PORT, HTTP_PORT_NAME},
};

pub fn build_group_listener(
    cluster: &ValidatedCluster,
    role: &AirflowRole,
    listener_class: ListenerClassName,
    listener_group_name: ListenerName,
) -> listener::v1alpha1::Listener {
    listener::v1alpha1::Listener {
        metadata: object_meta(
            cluster,
            listener_group_name,
            recommended_labels_for_role_resources(cluster, role),
        )
        .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(listener_class.to_string()),
            ports: Some(listener_ports()),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    }
}

/// We only use the http port here and intentionally omit
/// the metrics one.
fn listener_ports() -> Vec<listener::v1alpha1::ListenerPort> {
    vec![listener::v1alpha1::ListenerPort {
        name: HTTP_PORT_NAME.to_string(),
        port: HTTP_PORT.into(),
        protocol: Some("TCP".to_string()),
    }]
}
