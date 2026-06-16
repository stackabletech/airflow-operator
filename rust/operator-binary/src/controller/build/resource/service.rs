use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::{Annotations, Labels},
    v2::{builder::meta::ownerreference_from_resource, types::operator::RoleGroupName},
};

use crate::{
    controller::ValidatedCluster,
    crd::{AirflowRole, HTTP_PORT, HTTP_PORT_NAME, METRICS_PORT, METRICS_PORT_NAME},
};

/// The rolegroup headless [`Service`] is a service that allows direct access to the instances of a certain rolegroup
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub fn build_rolegroup_headless_service(
    cluster: &ValidatedCluster,
    role: &AirflowRole,
    role_group_name: &RoleGroupName,
) -> Service {
    Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(
                cluster
                    .resource_names(&role.role_name(), role_group_name)
                    .headless_service_name()
                    .to_string(),
            )
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(cluster.recommended_labels(role, role_group_name))
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(headless_service_ports()),
            selector: Some(cluster.role_group_selector(role, role_group_name).into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping label.
pub fn build_rolegroup_metrics_service(
    cluster: &ValidatedCluster,
    role: &AirflowRole,
    role_group_name: &RoleGroupName,
) -> Service {
    Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(
                cluster
                    .resource_names(&role.role_name(), role_group_name)
                    .metrics_service_name()
                    .to_string(),
            )
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(cluster.recommended_labels(role, role_group_name))
            .with_labels(prometheus_labels())
            .with_annotations(prometheus_annotations())
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(metrics_service_ports()),
            selector: Some(cluster.role_group_selector(role, role_group_name).into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

/// The headless [`Service`] name backing the StatefulSet (`<cluster>-<role>-<rolegroup>-headless`).
pub fn stateful_set_service_name(
    cluster: &ValidatedCluster,
    role: &AirflowRole,
    role_group_name: &RoleGroupName,
) -> Option<String> {
    Some(
        cluster
            .resource_names(&role.role_name(), role_group_name)
            .headless_service_name()
            .to_string(),
    )
}

fn headless_service_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(HTTP_PORT_NAME.to_string()),
        port: HTTP_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

fn metrics_service_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(METRICS_PORT_NAME.to_string()),
        port: METRICS_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

/// Common labels for Prometheus
fn prometheus_labels() -> Labels {
    Labels::try_from([("prometheus.io/scrape", "true")]).expect("should be a valid label")
}

/// Common annotations for Prometheus
///
/// These annotations can be used in a ServiceMonitor.
///
/// see also <https://github.com/prometheus-community/helm-charts/blob/prometheus-27.32.0/charts/prometheus/values.yaml#L983-L1036>
fn prometheus_annotations() -> Annotations {
    Annotations::try_from([
        ("prometheus.io/path".to_owned(), "/metrics".to_owned()),
        ("prometheus.io/port".to_owned(), METRICS_PORT.to_string()),
        ("prometheus.io/scheme".to_owned(), "http".to_owned()),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}
