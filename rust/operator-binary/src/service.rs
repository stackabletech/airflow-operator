use stackable_operator::{kube::Resource, role_utils::RoleGroupRef};

pub const HEADLESS_SERVICE_SUFFIX: &str = "headless";

// REVIEW: made generic so the new controller can call this with
// RoleGroupRef<ValidatedAirflowCluster> instead of RoleGroupRef<v1alpha2::AirflowCluster>
pub fn stateful_set_service_name<T: Resource>(
    rolegroup_ref: &RoleGroupRef<T>,
) -> Option<String> {
    Some(rolegroup_headless_service_name(
        &rolegroup_ref.object_name(),
    ))
}

fn rolegroup_headless_service_name(role_group_ref_object_name: &str) -> String {
    format!("{role_group_ref_object_name}-{HEADLESS_SERVICE_SUFFIX}")
}
