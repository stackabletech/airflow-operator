# Changelog

## [Unreleased]

### Added

- More CRD documentation ([#354]).

### Changed

- `operator-rs` `0.56.1` -> `0.57.0` ([#354]).
- Increase resource defaults ([#352]).
- Use new label builders ([#364]).

### Fixed

- BREAKING: Fixed various issues in the CRD structure. `clusterConfig.credentialsSecret` is now mandatory ([#353]).

### Removed

- [BREAKING] Removed legacy node affinity selectors ([#364]).

[#352]: https://github.com/stackabletech/airflow-operator/pull/352
[#353]: https://github.com/stackabletech/airflow-operator/pull/353
[#354]: https://github.com/stackabletech/airflow-operator/pull/354
[#364]: https://github.com/stackabletech/airflow-operator/pull/364

## [23.11.0] - 2023-11-24

### Added

- [BREAKING] Implement KubernetesExecutor ([#311]).
- Default stackableVersion to operator version ([#312]).
- Support PodDisruptionBudgets ([#330]).
- Added support for versions 2.6.3, 2.7.2 ([#334]).
- Support graceful shutdown ([#343]).

### Changed

- [BREAKING] Consolidated `spec.clusterConfig.authenticationConfig` to `spec.clusterConfig.authentication` which now takes a vector of AuthenticationClass references  ([#303]).
- `vector` `0.26.0` -> `0.33.0` ([#308], [#334]).
- `operator-rs` `0.44.0` -> `0.55.0` ([#308], [#330], [#334]).
- [BREAKING] Removed AirflowDB object, since it created some problems when reinstalling or upgrading an Airflow cluster. Instead, the initialization of the database was moved to the startup phase of each scheduler pod. To make sure the initialization does not run in parallel, the `PodManagementPolicy` of the scheduler StatefulSet was set to `OrderedReady`. The `.spec.clusterConfig.databaseInitialization` option was removed from the CRD, since it was just there to enable logging for the database initialization Job, which doesn't exist anymore ([#322]).

### Fixed

- BREAKING: Rename Service port name from `airflow` to `http` for consistency reasons. This change should normally not be breaking, as we only change the name, not the port. However, there might be some e.g. Ingresses that rely on the port name and need to be updated ([#316]).
- Fix user-supplied gitsync git-config settings ([#335]).

### Removed

- Removed support for 2.2.3, 2.2.4, 2.2.5, 2.4.1 ([#334]).

[#303]: https://github.com/stackabletech/airflow-operator/pull/303
[#308]: https://github.com/stackabletech/airflow-operator/pull/308
[#311]: https://github.com/stackabletech/airflow-operator/pull/311
[#312]: https://github.com/stackabletech/airflow-operator/pull/312
[#316]: https://github.com/stackabletech/airflow-operator/pull/316
[#330]: https://github.com/stackabletech/airflow-operator/pull/330
[#334]: https://github.com/stackabletech/airflow-operator/pull/334
[#335]: https://github.com/stackabletech/airflow-operator/pull/335
[#343]: https://github.com/stackabletech/airflow-operator/pull/343

## [23.7.0] - 2023-07-14

### Added

- Generate OLM bundle for Release 23.4.0 ([#270]).
- Fix LDAP tests for Openshift ([#270]).
- Missing CRD defaults for `status.conditions` field ([#277]).
- Support Airflow `2.6.1` ([#284]).
- Set explicit resources on all containers ([#289])
- Operator errors out when credentialsSecret is missing ([#293]).
- Support podOverrides ([#295]).

### Fixed

- Increase the size limit of the log volume ([#299]).

### Changed

- [BREAKING] Consolidated remaining top-level config options to `clusterConfig` ([#271]).
- `operator-rs` `0.40.2` -> `0.44.0` ([#272], [#299]).
- Use 0.0.0-dev product images for testing ([#274])
- Use testing-tools 0.2.0 ([#274])
- Added kuttl test suites ([#291])

[#270]: https://github.com/stackabletech/airflow-operator/pull/270
[#271]: https://github.com/stackabletech/airflow-operator/pull/271
[#272]: https://github.com/stackabletech/airflow-operator/pull/272
[#274]: https://github.com/stackabletech/airflow-operator/pull/274
[#277]: https://github.com/stackabletech/airflow-operator/pull/277
[#284]: https://github.com/stackabletech/airflow-operator/pull/284
[#289]: https://github.com/stackabletech/airflow-operator/pull/289
[#291]: https://github.com/stackabletech/airflow-operator/pull/291
[#293]: https://github.com/stackabletech/airflow-operator/pull/293
[#295]: https://github.com/stackabletech/airflow-operator/pull/295
[#299]: https://github.com/stackabletech/airflow-operator/pull/299

## [23.4.0] - 2023-04-17

### Added

- Log aggregation added ([#219]).
- Deploy default and support custom affinities ([#241]).
- Add the ability to loads DAG via git-sync ([#245]).
- Cluster status conditions ([#255])
- Extend cluster resources for status and cluster operation (paused, stopped) ([#257])
- Added more detailed landing page for the docs ([#260]).

### Changed

- [BREAKING] Support specifying Service type.
  This enables us to later switch non-breaking to using `ListenerClasses` for the exposure of Services.
  This change is breaking, because - for security reasons - we default to the `cluster-internal` `ListenerClass`.
  If you need your cluster to be accessible from outside of Kubernetes you need to set `clusterConfig.listenerClass`
  to `external-unstable` or `external-stable` ([#258]).
- `operator-rs` `0.31.0` -> `0.34.0` -> `0.39.0` -> `0.40.2` ([#219]) ([#257]) ([#261]).
- Specified security context settings needed for OpenShift ([#222]).
- Fixed template parsing for OpenShift tests ([#222]).
- Revert openshift settings ([#233]).
- Support crate2nix in dev environments ([#234]).
- Fixed LDAP tests on Openshift ([#254]).
- Reorganized usage guide docs([#260]).
- Set RBAC objects labels and owner ([#261])

### Removed

- Removed PVC-usage documentation ([#245]).

[#219]: https://github.com/stackabletech/airflow-operator/pull/219
[#222]: https://github.com/stackabletech/airflow-operator/pull/222
[#233]: https://github.com/stackabletech/airflow-operator/pull/233
[#234]: https://github.com/stackabletech/airflow-operator/pull/234
[#241]: https://github.com/stackabletech/airflow-operator/pull/241
[#245]: https://github.com/stackabletech/airflow-operator/pull/245
[#254]: https://github.com/stackabletech/airflow-operator/pull/254
[#255]: https://github.com/stackabletech/airflow-operator/pull/255
[#257]: https://github.com/stackabletech/airflow-operator/pull/257
[#258]: https://github.com/stackabletech/airflow-operator/pull/258
[#260]: https://github.com/stackabletech/airflow-operator/pull/260
[#261]: https://github.com/stackabletech/airflow-operator/pull/261

## [23.1.0] - 2023-01-23

### Changed

- `operator-rs` `0.25.2` -> `0.27.1` ([#197]).
- `operator-rs` `0.27.1` -> `0.30.1` ([#208])
- `operator-rs` `0.30.1` -> `0.31.0` ([#216]).
- Updated stackable image versions ([#193]).
- [BREAKING] Use Product image selection instead of version ([#206]).
  - `spec.version` has been replaced by `spec.image`.
  - `spec.statsdExporterVersion` has been removed, the statsd-exporter is now part of the images itself
- Fixed the RoleGroup `selector`. It was not used before. ([#208])
- Refactored LDAP related code to use new `LdapAuthenticationProvider` functionality ([#216])

[#197]: https://github.com/stackabletech/airflow-operator/pull/197
[#206]: https://github.com/stackabletech/airflow-operator/pull/206
[#208]: https://github.com/stackabletech/airflow-operator/pull/208
[#216]: https://github.com/stackabletech/airflow-operator/pull/216

## [0.6.0] - 2022-11-07

### Added

- Cpu and memory limits are now configurable ([#167]).
- Stale resources are now deleted ([#174]).
- Support for Airflow 2.4.1 ([#179]).

### Changed

- Toggle podSecurityContext for Openshift/Non-Openshift test scenarios ([#171])
- `operator-rs` `0.22.0` -> `0.25.2` ([#174])

[#167]: https://github.com/stackabletech/airflow-operator/pull/167
[#171]: https://github.com/stackabletech/airflow-operator/pull/171
[#174]: https://github.com/stackabletech/airflow-operator/pull/174
[#179]: https://github.com/stackabletech/airflow-operator/pull/179

## [0.5.0] - 2022-09-06

### Added

- LDAP user authentication ([#133]).
- Documentation for calling custom resources ([#155]).

### Changed

- OpenShift compatibility ([#127])
- Include chart name when installing with a custom release name ([#131], [#132]).
- Use correct webserver key to allow access to DAG logs from the Webserver UI ([#155]).
- Add LDAP readiness probe for tests ([#155]).

[#127]: https://github.com/stackabletech/airflow-operator/pull/127
[#131]: https://github.com/stackabletech/airflow-operator/pull/131
[#132]: https://github.com/stackabletech/airflow-operator/pull/132
[#133]: https://github.com/stackabletech/airflow-operator/pull/133
[#155]: https://github.com/stackabletech/airflow-operator/pull/155

## [0.4.0] - 2022-06-30

## [0.3.0] - 2022-05-31

### Added

- BREAKING: Prometheus metrics enabled ([#51]); The `statsdExporterVersion` must
  be set in the cluster specification.
- Reconciliation errors are now reported as Kubernetes events ([#53]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#54]).
- Support for Airflow 2.2.4, 2.2.5 documented ([#68],[#84]).
- Support for mounting DAGs via `ConfigMap` or `PersistentVolumeClaim` ([#84]).
- Init job replaced with an AirflowDB resource created by the operator  ([#89]).
- Stabilize start-up by watching AirflowDB job ([#104]).
- Stabilize integration tests ([#109]).

### Changed

- `operator-rs` `0.10.0` -> `0.18.0` ([#53],[#54],[#89]).
- [BREAKING] Specifying the product version has been changed to adhere to [ADR018](https://docs.stackable.tech/home/contributor/adr/ADR018-product_image_versioning.html) instead of just specifying the product version you will now have to add the Stackable image version as well, so `version: 3.5.8` becomes (for example) `version: 3.5.8-stackable0.1.0` ([#106])

[#51]: https://github.com/stackabletech/airflow-operator/pull/51
[#53]: https://github.com/stackabletech/airflow-operator/pull/53
[#54]: https://github.com/stackabletech/airflow-operator/pull/54
[#68]: https://github.com/stackabletech/airflow-operator/pull/68
[#84]: https://github.com/stackabletech/airflow-operator/pull/84
[#89]: https://github.com/stackabletech/airflow-operator/pull/89
[#104]: https://github.com/stackabletech/airflow-operator/pull/104
[#106]: https://github.com/stackabletech/airflow-operator/pull/106
[#109]: https://github.com/stackabletech/airflow-operator/pull/109

## [0.2.0] - 2022-02-14

### Changed
- Fixed a bug in the namespace resolution for the Init job that resulted in it not being triggered in non-default
namespaces. ([#23]).

[#23]: https://github.com/stackabletech/airflow-operator/pull/23

- Added comments about the override of configuration properties and environment variables, and added code to pass the environment variables in the custom resource to the container, as this step was missing ([#42]).

[#42]: https://github.com/stackabletech/airflow-operator/pull/42

## [0.1.0] - 2022-02-03

### Added

- Added the initial implementation of the operator. The Init command - which takes the credentials from a secret - is required to set up the external database, and the webserver service will wait for this to be completed before declaring itself to be ready. ([#1]).

[#1]: https://github.com/stackabletech/airflow-operator/pull/1
