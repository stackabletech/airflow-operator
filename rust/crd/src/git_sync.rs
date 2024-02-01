use serde::{Deserialize, Serialize};
use stackable_operator::{
    schemars::{self, JsonSchema},
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};
use std::collections::BTreeMap;

use crate::{GIT_LINK, GIT_ROOT, GIT_SAFE_DIR, GIT_SYNC_DEPTH, GIT_SYNC_WAIT};

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitSync {
    /// The git repository URL that will be cloned, for example: `https://github.com/stackabletech/airflow-operator`.
    pub repo: String,
    /// The branch to clone. Defaults to `main`.
    pub branch: Option<String>,
    /// The location of the DAG folder, relative to the synced repository root.
    pub git_folder: Option<String>,
    /// The depth of syncing i.e. the number of commits to clone; defaults to 1.
    pub depth: Option<u8>,
    /// The synchronization interval in seconds; defaults to 20 seconds.
    pub wait: Option<u16>,
    /// The name of the Secret used to access the repository if it is not public.
    /// This should include two fields: `user` and `password`.
    /// The `password` field can either be an actual password (not recommended) or a GitHub token,
    /// as described [here](https://github.com/kubernetes/git-sync/tree/v3.6.4#flags-which-configure-authentication).
    pub credentials_secret: Option<String>,
    /// A map of optional configuration settings that are listed in the [git-sync documentation](https://github.com/kubernetes/git-sync/tree/v3.6.4#primary-flags).
    /// Read the [git sync example](DOCS_BASE_URL_PLACEHOLDER/airflow/usage-guide/mounting-dags#_example).
    pub git_sync_conf: Option<BTreeMap<String, String>>,
}
impl GitSync {
    pub fn get_args(&self, one_time: bool) -> Vec<String> {
        let mut git_config = format!("{GIT_SAFE_DIR}:{GIT_ROOT}");
        let mut git_sync_command = vec![
            "/stackable/git-sync".to_string(),
            format!("--repo={}", self.repo.clone()),
            format!(
                "--branch={}",
                self.branch.clone().unwrap_or_else(|| "main".to_string())
            ),
            format!("--depth={}", self.depth.unwrap_or(GIT_SYNC_DEPTH)),
            format!("--wait={}", self.wait.unwrap_or(GIT_SYNC_WAIT)),
            format!("--dest={GIT_LINK}"),
            format!("--root={GIT_ROOT}"),
        ];
        if let Some(git_sync_conf) = self.git_sync_conf.as_ref() {
            for (key, value) in git_sync_conf {
                // config options that are internal details have
                // constant values and will be ignored here
                if key.eq_ignore_ascii_case("--dest") || key.eq_ignore_ascii_case("--root") {
                    tracing::warn!("Config option {:?} will be ignored...", key);
                } else {
                    // both "-git-config" and "--gitconfig" are recognized by gitsync
                    if key.to_lowercase().ends_with("-git-config") {
                        if value.to_lowercase().contains(GIT_SAFE_DIR) {
                            tracing::warn!("Config option {value:?} contains a value for {GIT_SAFE_DIR} that overrides
                                the value of this operator. Git-sync functionality will probably not work as expected!");
                        }
                        git_config = format!("{git_config},{value}");
                    } else {
                        git_sync_command.push(format!("{key}={value}"));
                    }
                }
            }
            git_sync_command.push(format!("--git-config='{git_config}'"));
        }

        let mut args: Vec<String> = vec![];

        if one_time {
            // for one-time git-sync calls (which is the case when git-sync runs as an init
            // container in a job created by the KubernetesExecutor), specify this with the relevant
            // parameter and do not push the process into the background
            git_sync_command.push("--one-time=true".to_string());
            args.push(git_sync_command.join(" "));
        } else {
            // otherwise, we need the signal termination code and the process pushed to the background
            git_sync_command.push("&".to_string());
            args.append(&mut vec![
                COMMON_BASH_TRAP_FUNCTIONS.to_string(),
                "prepare_signal_handlers".to_string(),
            ]);
            args.push(git_sync_command.join(" "));
            args.push("wait_for_termination $!".to_string());
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use crate::AirflowCluster;
    use rstest::rstest;

    #[test]
    fn test_git_sync() {
        let cluster = "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.7.2
          clusterConfig:
            loadExamples: false
            exposeConfig: false
            credentialsSecret: simple-airflow-credentials
            dagsGitSync:
              - name: git-sync
                repo: https://github.com/stackabletech/airflow-operator
                branch: feat/git-sync
                wait: 20
                gitSyncConf: {}
                gitFolder: tests/templates/kuttl/mount-dags-gitsync/dags
          webservers:
            roleGroups:
              default:
                config: {}
          celeryExecutors:
            roleGroups:
              default:
                config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ";

        let deserializer = serde_yaml::Deserializer::from_str(cluster);
        let cluster: AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        assert!(cluster.git_sync().is_some(), "git_sync was not Some!");
        assert_eq!(
            Some("tests/templates/kuttl/mount-dags-gitsync/dags".to_string()),
            cluster.git_sync().unwrap().git_folder
        );
    }

    #[test]
    fn test_git_sync_config() {
        let cluster = "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.7.2
          clusterConfig:
            loadExamples: false
            exposeConfig: false
            credentialsSecret: simple-airflow-credentials
            dagsGitSync:
              - name: git-sync
                repo: https://github.com/stackabletech/airflow-operator
                branch: feat/git-sync
                wait: 20
                gitSyncConf:
                  --rev: c63921857618a8c392ad757dda13090fff3d879a
                gitFolder: tests/templates/kuttl/mount-dags-gitsync/dags
          webservers:
            roleGroups:
              default:
                config: {}
          celeryExecutors:
            roleGroups:
              default:
                config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ";

        let deserializer = serde_yaml::Deserializer::from_str(cluster);
        let cluster: AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        assert!(cluster
            .git_sync()
            .unwrap()
            .get_args(false)
            .iter()
            .any(|c| c.contains("--rev=c63921857618a8c392ad757dda13090fff3d879a")));
    }

    #[rstest]
    #[case(
        "\"--git-config\": \"http.sslCAInfo:/tmp/ca-cert/ca.crt\"",
        "--git-config='safe.directory:/tmp/git,http.sslCAInfo:/tmp/ca-cert/ca.crt'"
    )]
    #[case(
        "\"-git-config\": \"http.sslCAInfo:/tmp/ca-cert/ca.crt\"",
        "--git-config='safe.directory:/tmp/git,http.sslCAInfo:/tmp/ca-cert/ca.crt'"
    )]
    #[case(
        "\"--git-config\": http.sslCAInfo:/tmp/ca-cert/ca.crt",
        "--git-config='safe.directory:/tmp/git,http.sslCAInfo:/tmp/ca-cert/ca.crt'"
    )]
    #[case(
        "--git-config: http.sslCAInfo:/tmp/ca-cert/ca.crt",
        "--git-config='safe.directory:/tmp/git,http.sslCAInfo:/tmp/ca-cert/ca.crt'"
    )]
    #[case(
        "'--git-config': 'http.sslCAInfo:/tmp/ca-cert/ca.crt'",
        "--git-config='safe.directory:/tmp/git,http.sslCAInfo:/tmp/ca-cert/ca.crt'"
    )]
    #[case(
    "--git-config: 'http.sslCAInfo:/tmp/ca-cert/ca.crt,safe.directory:/tmp/git2'",
    "--git-config='safe.directory:/tmp/git,http.sslCAInfo:/tmp/ca-cert/ca.crt,safe.directory:/tmp/git2'"
    )]
    fn test_git_sync_git_config(#[case] input: &str, #[case] output: &str) {
        let cluster = format!(
            "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          image:
            productVersion: 2.7.2
          clusterConfig:
            loadExamples: false
            exposeConfig: false
            credentialsSecret: simple-airflow-credentials
            dagsGitSync:
              - name: git-sync
                repo: https://github.com/stackabletech/airflow-operator
                branch: feat/git-sync
                wait: 20
                gitSyncConf:
                  {input}
                gitFolder: tests/templates/kuttl/mount-dags-gitsync/dags
          webservers:
            roleGroups:
              default:
                replicas: 1
          celeryExecutors:
            roleGroups:
              default:
                replicas: 1
          schedulers:
            roleGroups:
              default:
                replicas: 1
          "
        );

        let deserializer = serde_yaml::Deserializer::from_str(cluster.as_str());
        let cluster: AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        assert!(cluster
            .git_sync()
            .unwrap()
            .get_args(false)
            .iter()
            .any(|c| c.contains(output)));
    }
}
