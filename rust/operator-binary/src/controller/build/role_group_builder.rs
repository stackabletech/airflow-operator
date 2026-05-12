use std::collections::BTreeMap;

use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::{
        apps::v1::{StatefulSet, StatefulSetSpec},
        core::v1::{
            Affinity, ConfigMap, Container as K8sContainer, ContainerPort, PodSecurityContext,
            PodSpec, PodTemplateSpec, Probe, ResourceRequirements, TCPSocketAction, Volume,
            VolumeMount,
        },
    },
    kvp::{Annotation, Label, Labels},
    role_utils::RoleGroupRef,
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};
use stackable_operator::k8s_openapi::{
    DeepMerge,
    apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString},
};

use crate::{
    controller_commons::{self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
    crd::{
        AIRFLOW_CONFIG_FILENAME, APP_NAME, AirflowExecutor, AirflowRole, CONFIG_PATH, Container,
        HTTP_PORT_NAME, LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME, LOG_CONFIG_DIR, METRICS_PORT,
        METRICS_PORT_NAME, OPERATOR_NAME, STACKABLE_LOG_DIR, TEMPLATE_LOCATION,
        TEMPLATE_VOLUME_NAME,
    },
    framework::{
        self,
        builder::meta::ownerreference_from_resource,
        types::operator::*,
    },
    product_logging::extend_config_map_with_log_config,
    service::stateful_set_service_name,
};

use super::super::{
    AIRFLOW_CONTROLLER_NAME, PrecomputedPodData, ValidatedAirflowCluster,
    ValidatedRoleGroupConfig,
};

pub(crate) struct RoleGroupBuilder<'a> {
    cluster: &'a ValidatedAirflowCluster,
    role_group_config: &'a ValidatedRoleGroupConfig,
    rolegroup_ref: RoleGroupRef<ValidatedAirflowCluster>,
    airflow_role: AirflowRole,
    main_container: Container,
    pod_data: &'a PrecomputedPodData,
}

impl<'a> RoleGroupBuilder<'a> {
    pub(crate) fn new(
        cluster: &'a ValidatedAirflowCluster,
        role_group_config: &'a ValidatedRoleGroupConfig,
        rolegroup_ref: RoleGroupRef<ValidatedAirflowCluster>,
        airflow_role: AirflowRole,
        main_container: Container,
        pod_data: &'a PrecomputedPodData,
    ) -> Self {
        Self {
            cluster,
            role_group_config,
            rolegroup_ref,
            airflow_role,
            main_container,
            pod_data,
        }
    }

    pub(crate) fn build_config_map(&self) -> ConfigMap {
        let metadata = self
            .common_metadata(self.rolegroup_ref.object_name())
            .build();

        let mut cm_builder = ConfigMapBuilder::new();
        cm_builder.metadata(metadata);

        cm_builder.add_data(
            AIRFLOW_CONFIG_FILENAME,
            self.role_group_config.config_file_content.clone(),
        );

        extend_config_map_with_log_config(
            &self.rolegroup_ref,
            &self.main_container,
            &self.role_group_config.logging.airflow_container,
            self.role_group_config.logging.vector_container.as_ref(),
            &mut cm_builder,
            &self.cluster.image,
        );

        cm_builder
            .build()
            .expect("ConfigMap should build because metadata is set")
    }

    pub(crate) fn build_stateful_set(&self) -> StatefulSet {
        let restarter_label = Label::try_from(("restarter.stackable.tech/enabled", "true"))
            .expect("static label is always valid");

        let metadata = self
            .common_metadata(self.rolegroup_ref.object_name())
            .with_label(restarter_label)
            .build();

        let template = self.build_pod_template();

        let match_labels = {
            framework::kvp::label::role_group_selector(
                self.cluster,
                &ProductName::from_str_unsafe(APP_NAME),
                &RoleName::from_str_unsafe(&self.rolegroup_ref.role),
                &RoleGroupName::from_str_unsafe(&self.rolegroup_ref.role_group),
            )
        };

        let pod_management_policy = match self.airflow_role {
            AirflowRole::Scheduler => "OrderedReady",
            AirflowRole::Webserver
            | AirflowRole::Worker
            | AirflowRole::DagProcessor
            | AirflowRole::Triggerer => "Parallel",
        }
        .to_string();

        let spec = StatefulSetSpec {
            pod_management_policy: Some(pod_management_policy),
            replicas: self.pod_data.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(match_labels.into()),
                ..LabelSelector::default()
            },
            service_name: stateful_set_service_name(&self.rolegroup_ref),
            template,
            volume_claim_templates: self
                .pod_data
                .listener_volume_claim_template
                .clone()
                .map(|pvc| vec![pvc]),
            ..StatefulSetSpec::default()
        };

        StatefulSet {
            metadata,
            spec: Some(spec),
            status: None,
        }
    }

    fn build_pod_template(&self) -> PodTemplateSpec {
        let pod_metadata = ObjectMetaBuilder::new()
            .with_labels(self.recommended_labels())
            .with_annotation(
                Annotation::try_from((
                    "kubectl.kubernetes.io/default-container",
                    format!("{}", self.main_container),
                ))
                .expect("static annotation is always valid"),
            )
            .build();

        let airflow_container = self.build_airflow_container();
        let metrics_container = self.build_metrics_container();

        let mut containers = vec![airflow_container, metrics_container];
        containers.extend(self.pod_data.git_sync_containers.clone());
        if let Some(vector_container) = &self.pod_data.vector_container {
            containers.push(vector_container.clone());
        }

        let init_containers = if self.pod_data.git_sync_init_containers.is_empty() {
            None
        } else {
            Some(self.pod_data.git_sync_init_containers.clone())
        };

        let volumes = self.build_volumes();

        let termination_grace_period_seconds = self
            .role_group_config
            .graceful_shutdown_timeout
            .as_secs()
            .try_into()
            .ok();

        let mut pod_template = PodTemplateSpec {
            metadata: Some(pod_metadata),
            spec: Some(PodSpec {
                affinity: {
                    let a = &self.role_group_config.affinity;
                    if a.pod_affinity.is_some()
                        || a.pod_anti_affinity.is_some()
                        || a.node_affinity.is_some()
                    {
                        Some(Affinity {
                            pod_affinity: a.pod_affinity.clone(),
                            pod_anti_affinity: a.pod_anti_affinity.clone(),
                            node_affinity: a.node_affinity.clone(),
                        })
                    } else {
                        None
                    }
                },
                containers,
                init_containers,
                service_account_name: Some(self.pod_data.service_account_name.clone()),
                termination_grace_period_seconds,
                security_context: Some(PodSecurityContext {
                    fs_group: Some(1000),
                    ..PodSecurityContext::default()
                }),
                image_pull_secrets: self.cluster.image.pull_secrets.clone(),
                volumes: if volumes.is_empty() {
                    None
                } else {
                    Some(volumes)
                },
                ..PodSpec::default()
            }),
        };

        pod_template.merge_from(self.pod_data.pod_overrides.clone());
        pod_template
    }

    fn build_airflow_container(&self) -> K8sContainer {
        let mut volume_mounts = vec![
            VolumeMount {
                name: CONFIG_VOLUME_NAME.to_string(),
                mount_path: CONFIG_PATH.to_string(),
                ..VolumeMount::default()
            },
            VolumeMount {
                name: LOG_CONFIG_VOLUME_NAME.to_string(),
                mount_path: LOG_CONFIG_DIR.to_string(),
                ..VolumeMount::default()
            },
            VolumeMount {
                name: LOG_VOLUME_NAME.to_string(),
                mount_path: STACKABLE_LOG_DIR.to_string(),
                ..VolumeMount::default()
            },
        ];

        volume_mounts.extend(self.pod_data.extra_volume_mounts.clone());
        volume_mounts.extend(self.pod_data.auth_volume_mounts.clone());
        volume_mounts.extend(self.pod_data.git_sync_volume_mounts.clone());

        if matches!(
            self.pod_data.executor,
            AirflowExecutor::KubernetesExecutors { .. }
        ) {
            volume_mounts.push(VolumeMount {
                name: TEMPLATE_VOLUME_NAME.to_string(),
                mount_path: TEMPLATE_LOCATION.to_string(),
                ..VolumeMount::default()
            });
        }

        if self.airflow_role.get_http_port().is_some()
            && self.pod_data.listener_volume_claim_template.is_some()
        {
            volume_mounts.push(VolumeMount {
                name: LISTENER_VOLUME_NAME.to_string(),
                mount_path: LISTENER_VOLUME_DIR.to_string(),
                ..VolumeMount::default()
            });
        }

        let mut ports = Vec::new();
        if let Some(http_port) = self.airflow_role.get_http_port() {
            ports.push(ContainerPort {
                name: Some(HTTP_PORT_NAME.to_string()),
                container_port: http_port.into(),
                ..ContainerPort::default()
            });
        }

        let (readiness_probe, liveness_probe) =
            if let Some(http_port) = self.airflow_role.get_http_port() {
                let probe = Probe {
                    tcp_socket: Some(TCPSocketAction {
                        port: IntOrString::Int(http_port.into()),
                        ..TCPSocketAction::default()
                    }),
                    initial_delay_seconds: Some(60),
                    period_seconds: Some(10),
                    failure_threshold: Some(6),
                    ..Probe::default()
                };
                (Some(probe.clone()), Some(probe))
            } else {
                (None, None)
            };

        K8sContainer {
            name: self.main_container.to_string(),
            image: Some(self.cluster.image.image.clone()),
            image_pull_policy: Some(self.cluster.image.image_pull_policy.clone()),
            command: Some(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-euo".to_string(),
                "pipefail".to_string(),
                "-c".to_string(),
            ]),
            args: Some(vec![self.pod_data.airflow_commands.join("\n")]),
            env: Some(self.pod_data.env_vars.clone()),
            ports: if ports.is_empty() { None } else { Some(ports) },
            volume_mounts: Some(volume_mounts),
            resources: Some(self.role_group_config.resources.clone().into()),
            readiness_probe,
            liveness_probe,
            ..K8sContainer::default()
        }
    }

    fn build_metrics_container(&self) -> K8sContainer {
        let args = [
            COMMON_BASH_TRAP_FUNCTIONS.to_string(),
            "prepare_signal_handlers".to_string(),
            "/stackable/statsd_exporter &".to_string(),
            "wait_for_termination $!".to_string(),
        ]
        .join("\n");

        K8sContainer {
            name: "metrics".to_string(),
            image: Some(self.cluster.image.image.clone()),
            image_pull_policy: Some(self.cluster.image.image_pull_policy.clone()),
            command: Some(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-euo".to_string(),
                "pipefail".to_string(),
                "-c".to_string(),
            ]),
            args: Some(vec![args]),
            ports: Some(vec![ContainerPort {
                name: Some(METRICS_PORT_NAME.to_string()),
                container_port: METRICS_PORT.into(),
                ..ContainerPort::default()
            }]),
            resources: Some(ResourceRequirements {
                requests: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity("100m".to_string())),
                    ("memory".to_string(), Quantity("64Mi".to_string())),
                ])),
                limits: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity("200m".to_string())),
                    ("memory".to_string(), Quantity("64Mi".to_string())),
                ])),
                ..ResourceRequirements::default()
            }),
            ..K8sContainer::default()
        }
    }

    fn build_volumes(&self) -> Vec<Volume> {
        let mut volumes = controller_commons::create_volumes(
            &self.rolegroup_ref.object_name(),
            &self.role_group_config.logging.airflow_container,
        );

        volumes.extend(self.pod_data.extra_volumes.clone());
        volumes.extend(self.pod_data.auth_volumes.clone());
        volumes.extend(self.pod_data.git_sync_volumes.clone());

        if let Some(template_cm_name) = &self.pod_data.executor_template_configmap_name {
            volumes.push(Volume {
                name: TEMPLATE_VOLUME_NAME.to_string(),
                config_map: Some(
                    stackable_operator::k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                        name: template_cm_name.clone(),
                        ..Default::default()
                    },
                ),
                ..Volume::default()
            });
        }

        volumes
    }

    fn common_metadata(&self, resource_name: impl Into<String>) -> ObjectMetaBuilder {
        let mut builder = ObjectMetaBuilder::new();

        builder
            .name(resource_name)
            .namespace(&self.cluster.namespace)
            .ownerreference(ownerreference_from_resource(self.cluster, None, Some(true)))
            .with_labels(self.recommended_labels());

        builder
    }

    fn recommended_labels(&self) -> Labels {
        framework::kvp::label::recommended_labels(
            self.cluster,
            &ProductName::from_str_unsafe(APP_NAME),
            &ProductVersion::from_str_unsafe(
                &self.cluster.image.app_version_label_value.to_string(),
            ),
            &OperatorName::from_str_unsafe(OPERATOR_NAME),
            &ControllerName::from_str_unsafe(AIRFLOW_CONTROLLER_NAME),
            &RoleName::from_str_unsafe(&self.rolegroup_ref.role),
            &RoleGroupName::from_str_unsafe(&self.rolegroup_ref.role_group),
        )
    }
}
