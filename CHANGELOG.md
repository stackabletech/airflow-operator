# Changelog

## [Unreleased]

### Added

- Add a flag to determine if database initialization steps should be executed ([#669]).
- Add new roles for dag-processor and triggerer processes ([#679]).
- Added a note on webserver workers to the trouble-shooting section ([#685]).
- Helm: Allow Pod `priorityClassName` to be configured ([#687]).

### Changed

- Use internal secrets for secret- and jwt-keys ([#686]).
- Update uvicorn version and revert to default number of API workers ([#690]).

### Fixed

- Don't panic on invalid authorization config. Previously, a missing OPA ConfigMap would crash the operator ([#667]).
- Fix OPA authorization for Airflow 3. Airflow 3 needs to be configured via env variables, the operator now does this correctly ([#668]).
- Allow multiple Airflows in the same namespace to use Kubernetes executors.
  Previously, the operator would always use the same name for the executor Pod template ConfigMap.
  Thus when deploying multiple Airflow instances in the same namespace, there would be a conflict over the contents of that ConfigMap ([#678]).
- For versions >= 3 custom logging initializes the RemoteLogIO handler to fix remote logging ([#683]).

[#667]: https://github.com/stackabletech/airflow-operator/pull/667
[#668]: https://github.com/stackabletech/airflow-operator/pull/668
[#669]: https://github.com/stackabletech/airflow-operator/pull/669
[#678]: https://github.com/stackabletech/airflow-operator/pull/678
[#679]: https://github.com/stackabletech/airflow-operator/pull/679
[#683]: https://github.com/stackabletech/airflow-operator/pull/683
[#685]: https://github.com/stackabletech/airflow-operator/pull/685
[#686]: https://github.com/stackabletech/airflow-operator/pull/686
[#687]: https://github.com/stackabletech/airflow-operator/pull/687
[#690]: https://github.com/stackabletech/airflow-operator/pull/690

## [25.7.0] - 2025-07-23

## [25.7.0-rc1] - 2025-07-18

### Added

- Added listener support for Airflow ([#604]).
- Adds new telemetry CLI arguments and environment variables ([#613]).
  - Use `--file-log-max-files` (or `FILE_LOG_MAX_FILES`) to limit the number of log files kept.
  - Use `--file-log-rotation-period` (or `FILE_LOG_ROTATION_PERIOD`) to configure the frequency of rotation.
  - Use `--console-log-format` (or `CONSOLE_LOG_FORMAT`) to set the format to `plain` (default) or `json`.
- Add support for airflow `2.10.5` ([#625]).
- Add experimental support for airflow `3.0.1` ([#630]).
- "airflow.task" logger defaults to log level 'INFO' instead of 'NOTSET' ([#649]).
- Add internal headless service in addition to the metrics service ([#651]).
- Add RBAC rule to helm template for automatic cluster domain detection ([#656]).

### Changed

- BREAKING: Replace stackable-operator `initialize_logging` with stackable-telemetry `Tracing` ([#601], [#608], [#613]).
  - The console log level was set by `AIRFLOW_OPERATOR_LOG`, and is now set by `CONSOLE_LOG_LEVEL`.
  - The file log level was set by `AIRFLOW_OPERATOR_LOG`, and is now set by `FILE_LOG_LEVEL`.
  - The file log directory was set by `AIRFLOW_OPERATOR_LOG_DIRECTORY`, and is now set
    by `FILE_LOG_DIRECTORY` (or via `--file-log-directory <DIRECTORY>`).
  - Replace stackable-operator `print_startup_string` with `tracing::info!` with fields.
- BREAKING: Inject the vector aggregator address into the vector config using the env var `VECTOR_AGGREGATOR_ADDRESS` instead
    of having the operator write it to the vector config ([#600]).
- test: Bump to Vector 0.46.1 ([#620]).
- test: Bump OPA to `1.4.2` ([#624]).
- Deprecate airflow `2.10.4` ([#625]).
- Move the git-sync implementation to operator-rs ([#623]). The functionality should not have changed.
- BREAKING: Previously this operator would hardcode the UID and GID of the Pods being created to 1000/0, this has changed now ([#636])
  - The `runAsUser` and `runAsGroup` fields will not be set anymore by the operator
  - The defaults from the docker images itself will now apply, which will be different from 1000/0 going forward
  - This is marked as breaking because tools and policies might exist, which require these fields to be set
- Changed listener class to be role-only ([#645]).
- BREAKING: Bump stackable-operator to 0.94.0 and update other dependencies ([#656]).
  - The default Kubernetes cluster domain name is now fetched from the kubelet API unless explicitly configured.
  - This requires operators to have the RBAC permission to get nodes/proxy in the apiGroup "". The helm-chart takes care of this.
  - The CLI argument `--kubernetes-node-name` or env variable `KUBERNETES_NODE_NAME` needs to be set. The helm-chart takes care of this.
- The operator helm-chart now grants RBAC `patch` permissions on `events.k8s.io/events`,
  so events can be aggregated (e.g. "error happened 10 times over the last 5 minutes") ([#660]).

### Fixed

- Use `json` file extension for log files ([#607]).
- Fix a bug where changes to ConfigMaps that are referenced in the AirflowCluster spec didn't trigger a reconciliation ([#600]).
- Allow uppercase characters in domain names ([#656]).

### Removed

- Remove the `lastUpdateTime` field from the stacklet status ([#656]).
- Remove role binding to legacy service accounts ([#656]).

[#600]: https://github.com/stackabletech/airflow-operator/pull/600
[#601]: https://github.com/stackabletech/airflow-operator/pull/601
[#604]: https://github.com/stackabletech/airflow-operator/pull/604
[#607]: https://github.com/stackabletech/airflow-operator/pull/607
[#608]: https://github.com/stackabletech/airflow-operator/pull/608
[#613]: https://github.com/stackabletech/airflow-operator/pull/613
[#620]: https://github.com/stackabletech/airflow-operator/pull/620
[#623]: https://github.com/stackabletech/airflow-operator/pull/623
[#624]: https://github.com/stackabletech/airflow-operator/pull/624
[#625]: https://github.com/stackabletech/airflow-operator/pull/625
[#630]: https://github.com/stackabletech/airflow-operator/pull/630
[#636]: https://github.com/stackabletech/airflow-operator/pull/636
[#645]: https://github.com/stackabletech/airflow-operator/pull/645
[#649]: https://github.com/stackabletech/airflow-operator/pull/649
[#651]: https://github.com/stackabletech/airflow-operator/pull/651
[#656]: https://github.com/stackabletech/airflow-operator/pull/656
[#660]: https://github.com/stackabletech/airflow-operator/pull/660

## [25.3.0] - 2025-03-21

### Added

- Run a `containerdebug` process in the background of each Airflow container to collect debugging information ([#557]).
- Aggregate emitted Kubernetes events on the CustomResources ([#571]).
- Add OPA support ([#573]).
- Add support for `2.10.4` ([#594]).

### Changed

- Default to OCI for image metadata and product image selection ([#572]).
- BREAKING: The field `.spec.clusterConfig.dagsGitSync[].wait` changed from `uint8` to our human-readable `Duration` struct.
  In case you have used `wait: 20` before, you need to change it to `wait: 20s` ([#596]).
- The field `.spec.clusterConfig.dagsGitSync[].depth` was promoted from `uint8` to `uint32` to allow for more cloning depth.
  This is a non-breaking change as all previous values are still valid ([#596]).

### Removed

- Remove support for `2.9.2` and `2.10.2` (experimental) ([#594]).

### Fixed

- Fix `git-sync` functionality in case no `gitFolder` is specified.
  The `gitFolder` field is now non-nullable, but has a default value, resulting in no breaking change ([#596]).
- Fix configOverrides by applying after defaults ([#597]).

[#557]: https://github.com/stackabletech/airflow-operator/pull/557
[#571]: https://github.com/stackabletech/airflow-operator/pull/571
[#572]: https://github.com/stackabletech/airflow-operator/pull/572
[#573]: https://github.com/stackabletech/airflow-operator/pull/573
[#594]: https://github.com/stackabletech/airflow-operator/pull/594
[#596]: https://github.com/stackabletech/airflow-operator/pull/596
[#597]: https://github.com/stackabletech/airflow-operator/pull/597

## [24.11.1] - 2025-01-09

### Fixed

- BREAKING: Use distinct ServiceAccounts for the Stacklets, so that multiple Stacklets can be
  deployed in one namespace. Existing Stacklets will use the newly created ServiceAccounts after
  restart ([#545]).
- Fix OIDC endpoint construction in case the `rootPath` does not have a trailing slash ([#547]).

[#545]: https://github.com/stackabletech/airflow-operator/pull/545
[#547]: https://github.com/stackabletech/airflow-operator/pull/547

## [24.11.0] - 2024-11-18

### Added

- Allowing arbitrary python code as `EXPERIMENTAL_FILE_HEADER` and `EXPERIMENTAL_FILE_FOOTER` in `webserver_config.py` ([#493]).
- The operator can now run on Kubernetes clusters using a non-default cluster domain.
  Use the env var `KUBERNETES_CLUSTER_DOMAIN` or the operator Helm chart property `kubernetesClusterDomain` to set a non-default cluster domain ([#518]).
- Support for `2.9.3` ([#494]).
- Experimental Support for `2.10.2` ([#512]).
- Add support for OpenID Connect ([#524], [#530])

### Changed

- Reduce CRD size from `1.7MB` to `111KB` by accepting arbitrary YAML input instead of the underlying schema for the following fields ([#488]):
  - `podOverrides`
  - `affinity`
  - `volumes`
  - `volumeMounts`
- Deprecate `2.9.2`, remove `2.6.x` and `2.8.x` ([#494]).

### Fixed

- Pass gitsync credentials through properly and use a fine-grained access token ([#489]).
- Failing to parse one `AirflowCluster`/`AuthenticationClass` should no longer cause the whole operator to stop functioning ([#520]).

[#488]: https://github.com/stackabletech/airflow-operator/pull/488
[#489]: https://github.com/stackabletech/airflow-operator/pull/489
[#493]: https://github.com/stackabletech/airflow-operator/pull/493
[#494]: https://github.com/stackabletech/airflow-operator/pull/494
[#518]: https://github.com/stackabletech/airflow-operator/pull/518
[#520]: https://github.com/stackabletech/airflow-operator/pull/520
[#524]: https://github.com/stackabletech/airflow-operator/pull/524
[#530]: https://github.com/stackabletech/airflow-operator/pull/530

## [24.7.0] - 2024-07-24

### Added

- Support for modularized DAGs ([#404]).
- Support for 2.8.4 and 2.9.2 ([#461]).

### Changed

- Bump `stackable-operator` from `0.64.0` to `0.70.0` ([#462]).
- Bump `product-config` from `0.6.0` to `0.7.0` ([#462]).
- Bump other dependencies ([#464]).

[#462]: https://github.com/stackabletech/airflow-operator/pull/462
[#464]: https://github.com/stackabletech/airflow-operator/pull/464

### Fixed

- Add missing affinities for Kubernetes executors ([#439]).
- Remove requirement of celery configs when using kubernetes executors ([#445]).
- Processing of corrupted log events fixed; If errors occur, the error
  messages are added to the log event ([#449]).
- Add volumes/volumeMounts/envOverrides to gitsync containers ([#456]).
- Removed support for 2.7.2 and 2.7.3 ([#461]).
- Prevent double logging of the airflow container logs ([#474]).

[#404]: https://github.com/stackabletech/airflow-operator/pull/404
[#439]: https://github.com/stackabletech/airflow-operator/pull/439
[#445]: https://github.com/stackabletech/airflow-operator/pull/445
[#449]: https://github.com/stackabletech/airflow-operator/pull/449
[#456]: https://github.com/stackabletech/airflow-operator/pull/456
[#461]: https://github.com/stackabletech/airflow-operator/pull/461
[#474]: https://github.com/stackabletech/airflow-operator/pull/474

## [24.3.0] - 2024-03-20

### Added

- More CRD documentation ([#354]).
- Helm: support labels in values.yaml ([#374]).
- Support for version `2.7.3`, `2.8.1` ([#387]).

### Changed

- `operator-rs` `0.56.1` -> `0.57.0` ([#354]).
- Increase resource defaults ([#352]).
- Use new label builders ([#366]).
- Use new ldap::AuthenticationClassProvider `endpoint_url()` method ([#366]).
- Support git-sync `4.2.1` ([#387]).
- Use a lightweight DAG for the getting started guide to avoid OOM issues ([#401])
- Raise the default readiness and liveness probe timeouts of the webserver to 120s ([#402])
  Also raise the memory request of the webserver from 2Gi to 3Gi.

### Fixed

- BREAKING: Fixed various issues in the CRD structure. `clusterConfig.credentialsSecret` is now mandatory ([#353]).
- Fixed ordering of variables written to the kubernetes executor pod template ([#372]).
- Fixed git-sync container running with KubernetesExecutor ([#381]).
- Add missing `pods/log` RBAC permission for airflow. Previously this caused brief error
  messages in the airflow task logs (`User "system:serviceaccount:default:airflow-serviceaccount" cannot get resource "pods/log" in API group "" in the namespace "default".`) ([#406]).

### Removed

- [BREAKING] Removed legacy node selector on roleGroups ([#366]).
- Removed support for version `2.6.1`, `2.7.1` ([#387]).

[#352]: https://github.com/stackabletech/airflow-operator/pull/352
[#353]: https://github.com/stackabletech/airflow-operator/pull/353
[#354]: https://github.com/stackabletech/airflow-operator/pull/354
[#366]: https://github.com/stackabletech/airflow-operator/pull/366
[#372]: https://github.com/stackabletech/airflow-operator/pull/372
[#374]: https://github.com/stackabletech/airflow-operator/pull/374
[#381]: https://github.com/stackabletech/airflow-operator/pull/381
[#387]: https://github.com/stackabletech/airflow-operator/pull/387
[#401]: https://github.com/stackabletech/airflow-operator/pull/401
[#402]: https://github.com/stackabletech/airflow-operator/pull/402
[#406]: https://github.com/stackabletech/airflow-operator/pull/406

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
