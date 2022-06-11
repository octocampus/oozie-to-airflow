import shlex

from airflow import models
from airflow.operators import bash
from airflow.providers.apache.spark.operators import spark_submit
from airflow.utils import dates

from o2a.o2a_libs import functions
from o2a.o2a_libs.property_utils import PropertySet

CONFIG = {
    "dataproc_cluster": "{{DATAPROC_CLUSTER_NAME}}",
    "gcp_conn_id": "google_cloud_default",
    "gcp_region": "{{GCP_REGION}}",
    "gcp_uri_prefix": "gs://{{COMPOSER_DAG_BUCKET}}/dags",
    "spark_conn_id": '"spark_default_conn"',
}

JOB_PROPS = {
    "user.name": "mac",
    "nameNode": "hdfs://",
    "resourceManager": "localhost:8032",
    "master": "local[*]",
    "queueName": "default",
    "examplesRoot": "examples",
    "oozie.use.system.libpath": "true",
    "oozie.wf.application.path": "${nameNode}/user/${user.name}/${examplesRoot}/apps/spark",
}

TASK_MAP = {"spark-node": ["spark-node_prepare", "spark-node"]}

TEMPLATE_ENV = {**CONFIG, **JOB_PROPS, "functions": functions, "task_map": TASK_MAP}

with models.DAG(
    "spark",
    schedule_interval=None,  # Change to suit your needs
    start_date=dates.days_ago(0),  # Change to suit your needs
    user_defined_macros=TEMPLATE_ENV,
) as dag:

    spark_node_prepare = bash.BashOperator(
        task_id="spark-node_prepare",
        trigger_rule="one_success",
        bash_command="hadoop fs  -rm -r -f  %s ;"
        % (
            shlex.quote(
                "/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/spark/lib/oozie-examples-4.3.0-copy.jar"
            )
        ),
        params=PropertySet(config=CONFIG, job_properties=JOB_PROPS).merged,
    )

    spark_node = spark_submit.SparkSubmitOperator(
        task_id="spark-node",
        trigger_rule="one_success",
        conf=PropertySet(config=CONFIG, job_properties=JOB_PROPS).xml_escaped.merged,
        name="Spark-FileCopy",
        java_class="org.apache.oozie.example.SparkFileCopy",
        conn_id='"spark_default_conn"',
        jars="{{nameNode}}/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/spark/lib/oozie-examples-4.3.0.jar",
        application_args=[
            "/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/spark/lib/oozie-examples-4.3.0.jar",
            "/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/spark/lib/oozie-examples-4.3.0-copy.jar",
        ],
    )

    spark_node_prepare >> spark_node
