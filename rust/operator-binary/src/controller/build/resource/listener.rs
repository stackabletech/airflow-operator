use stackable_operator::{
    builder::meta::ObjectMetaBuilder, crd::listener,
    v2::builder::meta::ownerreference_from_resource,
};

use crate::{
    controller::ValidatedCluster,
    crd::{AirflowRole, HTTP_PORT, HTTP_PORT_NAME},
};

pub fn build_group_listener(
    cluster: &ValidatedCluster,
    role: &AirflowRole,
    listener_class: String,
    listener_group_name: String,
) -> listener::v1alpha1::Listener {
    listener::v1alpha1::Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(listener_group_name)
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            // The group listener is a role-level object, so a constant `none` role-group is used
            // as the role-group label value.
            .with_labels(cluster.recommended_labels_for(
                &role.role_name(),
                &"none".parse().expect("'none' is a valid role group name"),
            ))
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(listener_class),
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
