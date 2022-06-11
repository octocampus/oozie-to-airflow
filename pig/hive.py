import shlex

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.operators import bash_operator
from airflow.utils import dates

from o2a.o2a_libs import functions
from o2a.o2a_libs.property_utils import PropertySet

CONFIG = {}

JOB_PROPS = {
    "user.name": "cedric",
    "nameNode": "hdfs://",
    "resourceManager": "localhost:8032",
    "queueName": "default",
    "examplesRoot": "examples",
    "oozie.use.system.libpath": "true",
    "oozie.wf.application.path": "${nameNode}/user/${user.name}/${examplesRoot}/apps/pig",
}

TASK_MAP = {"pig-node": ["pig-node_prepare", "pig-node"]}

TEMPLATE_ENV = {**CONFIG, **JOB_PROPS, "functions": functions, "task_map": TASK_MAP}

with models.DAG(
    "hive",
    schedule_interval=None,  # Change to suit your needs
    start_date=dates.days_ago(0),  # Change to suit your needs
    user_defined_macros=TEMPLATE_ENV,
) as dag:

    pig_node_prepare = bash_operator.BashOperator(
        task_id="pig-node_prepare",
        trigger_rule="one_success",
        bash_command="$DAGS_FOLDER/../data/prepare.sh "
        "-c %s -r %s "
        "-d %s "
        "-m %s "
        % (
            CONFIG["dataproc_cluster"],
            CONFIG["gcp_region"],
            shlex.quote("/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/pig/output-data"),
            shlex.quote("/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/pig/created-folder"),
        ),
        params=PropertySet(config=CONFIG, job_properties=JOB_PROPS).merged,
    )
    pig_node = dataproc_operator.DataProcPigOperator(
        task_id="pig-node",
        trigger_rule="one_success",
        query_uri="%s/%s" % (CONFIG["gcp_uri_prefix"], "id.pig"),
        variables={
            "INPUT": "/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/pig/input-data/test-data.txt",
            "OUTPUT": "/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/pig/output-data/",
        },
        dataproc_pig_properties=PropertySet(
            config=CONFIG,
            job_properties=JOB_PROPS,
            action_node_properties={
                "mapred.job.queue.name": "{{queueName}}",
                "mapred.map.output.compress": "false",
            },
        ).xml_escaped.merged,
        cluster_name=CONFIG["dataproc_cluster"],
        gcp_conn_id=CONFIG["gcp_conn_id"],
        region=CONFIG["gcp_region"],
        dataproc_job_id="pig-node",
        params=PropertySet(
            config=CONFIG,
            job_properties=JOB_PROPS,
            action_node_properties={
                "mapred.job.queue.name": "{{queueName}}",
                "mapred.map.output.compress": "false",
            },
        ).merged,
    )
    pig_node_prepare.set_downstream(pig_node)
