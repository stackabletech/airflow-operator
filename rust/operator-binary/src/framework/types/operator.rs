//! Names for operators
//!
//! Several types below have operator-specific max_length values and examples that are
//! hardcoded for airflow. When this module moves to operator-rs, these should be
//! parameterised so each operator can supply its own limits. The compile-time assertions
//! in role_group_utils.rs verify that the limits are consistent with each other.

use std::str::FromStr;

use crate::attributed_string_type;

attributed_string_type! {
    ProductName,
    "The name of a product",
    "airflow",
    // A suffix is added to produce a label value. An according compile-time check ensures that
    // max_length cannot be set higher.
    (max_length = 54),
    is_rfc_1123_dns_subdomain_name,
    is_valid_label_value
}

attributed_string_type! {
    ProductVersion,
    "The version of a product",
    "2.10.4",
    is_valid_label_value
}

attributed_string_type! {
    ClusterName,
    "The name of a cluster/stacklet",
    "my-airflow-cluster",
    // Suffixes are added to produce resource names. According compile-time checks ensure that
    // max_length cannot be set higher. Reduced from opensearch's 24 to 22 because airflow's
    // longest role name ("dagprocessor") is 12 chars vs opensearch's 10.
    (max_length = 22),
    is_rfc_1035_label_name,
    is_valid_label_value
}

attributed_string_type! {
    ControllerName,
    "The name of a controller in an operator",
    "airflowcluster",
    is_valid_label_value
}

attributed_string_type! {
    OperatorName,
    "The name of an operator",
    "airflow.stackable.tech",
    is_valid_label_value
}

attributed_string_type! {
    RoleGroupName,
    "The name of a role-group name",
    "default",
    // The role-group name is used to produce resource names. To make sure that all resource names
    // are valid, max_length is restricted. Compile-time checks ensure that max_length cannot be
    // set higher if not other names like the RoleName are set lower accordingly.
    (max_length = 16),
    is_rfc_1123_label_name,
    is_valid_label_value
}

attributed_string_type! {
    RoleName,
    "The name of a role name",
    "webserver",
    // The role name is used to produce resource names. To make sure that all resource names are
    // valid, max_length is restricted. Compile-time checks ensure that max_length cannot be set
    // higher if not other names like the RoleGroupName are set lower accordingly.
    (max_length = 12),
    is_rfc_1123_label_name,
    is_valid_label_value
}

#[cfg(test)]
mod tests {
    use super::{
        ClusterName, ControllerName, OperatorName, ProductName, ProductVersion, RoleGroupName,
        RoleName,
    };

    #[test]
    fn test_attributed_string_type_examples() {
        ProductName::test_example();
        ProductVersion::test_example();
        ClusterName::test_example();
        ControllerName::test_example();
        OperatorName::test_example();
        RoleGroupName::test_example();
        RoleName::test_example();
    }
}
