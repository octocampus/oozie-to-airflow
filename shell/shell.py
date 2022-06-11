import shlex

from airflow import models
from airflow.operators import bash
from airflow.utils import dates

from o2a.o2a_libs import functions
from o2a.o2a_libs.property_utils import PropertySet

CONFIG = {}

JOB_PROPS = {
    "user.name": "mac",
    "nameNode": "hdfs://localhost:8020",
    "resourceManager": "localhost:8032",
    "queueName": "default",
    "examplesRoot": "examples",
    "oozie.wf.application.path": "${nameNode}/user/${user.name}/${examplesRoot}/apps/shell",
}

TASK_MAP = {"shell1-node": ["shell1-node_prepare", "shell1-node"], "shell2-node": ["shell2-node"]}

TEMPLATE_ENV = {**CONFIG, **JOB_PROPS, "functions": functions, "task_map": TASK_MAP}

with models.DAG(
    "shell",
    schedule_interval=None,  # Change to suit your needs
    start_date=dates.days_ago(0),  # Change to suit your needs
    user_defined_macros=TEMPLATE_ENV,
) as dag:

    shell1_node_prepare = bash.BashOperator(
        task_id="shell1-node_prepare",
        trigger_rule="one_success",
        bash_command="hadoop fs  -mkdir  %s ;"
        % (shlex.quote("/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/shell/test"))
        + "hadoop fs  -rm -r -f  %s ;"
        % (shlex.quote("/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/shell/test")),
        params=PropertySet(config=CONFIG, job_properties=JOB_PROPS).merged,
    )

    shell1_node = bash.BashOperator(
        task_id="shell1-node",
        trigger_rule="one_success",
        bash_command=shlex.quote("java -version"),
    )

    shell1_node_prepare >> shell1_node

    shell2_node = bash.BashOperator(
        task_id="shell2-node",
        trigger_rule="one_success",
        bash_command=shlex.quote("echo $IP_ADDR"),
        env={"IP_ADDR": "127.0.0.1"},
    )

    shell1_node >> shell2_node
