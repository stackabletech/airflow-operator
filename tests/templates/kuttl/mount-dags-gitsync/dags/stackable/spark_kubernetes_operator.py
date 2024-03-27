from typing import TYPE_CHECKING, Optional, Sequence
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
import json

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SparkKubernetesOperator(BaseOperator):
    template_fields: Sequence[str] = ('application_file', 'namespace')
    template_ext: Sequence[str] = ('.yaml', '.yml', '.json')
    ui_color = '#f4a460'

    def __init__(
            self,
            *,
            application_file: str,
            namespace: Optional[str] = None,
            kubernetes_conn_id: str = 'kubernetes_in_cluster',
            api_group: str = 'spark.stackable.tech',
            api_version: str = 'v1alpha1',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.plural = "sparkapplications"

    def execute(self, context: 'Context'):
        hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        self.log.info("Creating SparkApplication...")
        self.log.info(json.dumps(self.application_file, indent=4))
        response = hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=self.application_file,
            namespace=self.namespace,
        )
        return response
