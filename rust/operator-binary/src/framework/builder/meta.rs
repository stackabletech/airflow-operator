use stackable_operator::{
    builder::meta::OwnerReferenceBuilder,
    k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference, kube::Resource,
};

use crate::framework::{HasName, HasUid};

/// Infallible variant of
/// [`stackable_operator::builder::meta::ObjectMetaBuilder::ownerreference_from_resource`]
pub fn ownerreference_from_resource(
    resource: &(impl Resource<DynamicType = ()> + HasName + HasUid),
    block_owner_deletion: Option<bool>,
    controller: Option<bool>,
) -> OwnerReference {
    OwnerReferenceBuilder::new()
        // Set api_version, kind, name and additionally the UID if it exists.
        .initialize_from_resource(resource)
        // Ensure that the name is set.
        .name(resource.to_name())
        // Ensure that the UID is set.
        .uid(resource.to_uid().to_string())
        .block_owner_deletion_opt(block_owner_deletion)
        .controller_opt(controller)
        .build()
        .expect(
            "OwnerReference should be created because the resource has an api_version, kind, name \
            and uid.",
        )
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use stackable_operator::{
        k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta, kube::Resource,
    };

    use crate::framework::{
        HasName, HasUid, builder::meta::ownerreference_from_resource, types::kubernetes::Uid,
    };

    struct TestCluster {
        object_meta: ObjectMeta,
    }

    impl TestCluster {
        fn new() -> Self {
            TestCluster {
                object_meta: ObjectMeta {
                    name: Some("test-cluster".to_owned()),
                    uid: Some("a6b89911-d48e-4328-88d6-b9251226583d".to_owned()),
                    ..ObjectMeta::default()
                },
            }
        }
    }

    impl Resource for TestCluster {
        type DynamicType = ();
        type Scope = ();

        fn kind(_dt: &Self::DynamicType) -> Cow<'_, str> {
            Cow::from("AirflowCluster")
        }

        fn group(_dt: &Self::DynamicType) -> Cow<'_, str> {
            Cow::from("airflow.stackable.tech")
        }

        fn version(_dt: &Self::DynamicType) -> Cow<'_, str> {
            Cow::from("v1alpha2")
        }

        fn plural(_dt: &Self::DynamicType) -> Cow<'_, str> {
            Cow::from("airflowclusters")
        }

        fn meta(&self) -> &ObjectMeta {
            &self.object_meta
        }

        fn meta_mut(&mut self) -> &mut ObjectMeta {
            &mut self.object_meta
        }
    }

    impl HasName for TestCluster {
        fn to_name(&self) -> String {
            self.object_meta.name.clone().expect("set in new()")
        }
    }

    impl HasUid for TestCluster {
        fn to_uid(&self) -> Uid {
            Uid::from_str_unsafe(&self.object_meta.uid.clone().expect("set in new()"))
        }
    }

    #[test]
    fn test_ownerreference_from_resource() {
        let owner_ref = ownerreference_from_resource(&TestCluster::new(), Some(true), Some(true));
        assert_eq!(owner_ref.name, "test-cluster");
        assert_eq!(owner_ref.uid, "a6b89911-d48e-4328-88d6-b9251226583d");
        assert_eq!(owner_ref.controller, Some(true));
        assert_eq!(owner_ref.block_owner_deletion, Some(true));
    }
}
