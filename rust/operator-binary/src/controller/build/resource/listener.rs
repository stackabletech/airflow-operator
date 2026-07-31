use std::str::FromStr;

use stackable_operator::{
    crd::listener,
    v2::types::{
        kubernetes::{ListenerClassName, ListenerName},
        operator::RoleGroupName,
    },
};

use crate::{
    controller::ValidatedCluster,
    crd::{AirflowRole, HTTP_PORT, HTTP_PORT_NAME},
};

// The group listener is a role-level object, so a constant `none` role-group is used as the
// role-group label value.
stackable_operator::constant!(NONE_ROLE_GROUP_NAME: RoleGroupName = "none");

pub fn build_group_listener(
    cluster: &ValidatedCluster,
    role: &AirflowRole,
    listener_class: ListenerClassName,
    listener_group_name: ListenerName,
) -> listener::v1alpha1::Listener {
    listener::v1alpha1::Listener {
        metadata: cluster
            .object_meta(
                listener_group_name,
                cluster.recommended_labels_for(
                    &ValidatedCluster::role_name(role),
                    &NONE_ROLE_GROUP_NAME,
                ),
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
