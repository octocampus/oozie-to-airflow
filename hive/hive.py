import shlex

from airflow import models
from airflow.operators import bash
from airflow.providers.apache.hive.operators import hive
from airflow.utils import dates

from o2a.o2a_libs import functions
from o2a.o2a_libs.property_utils import PropertySet

CONFIG = {"hive_cli_conn_id": '"hive_local"'}

JOB_PROPS = {
    "user.name": "mac",
    "nameNode": "hdfs://",
    "resourceManager": "localhost:8032",
    "queueName": "default",
    "examplesRoot": "examples",
    "oozie.use.system.libpath": "true",
    "oozie.wf.application.path": "${nameNode}/user/${user.name}/${examplesRoot}/apps/hive",
}

TASK_MAP = {
    "hive-script": ["hive-script"],
    "hive-query": ["hive-query_prepare", "hive-query"],
    "hive2-script": ["hive2-script"],
    "hive2-query": ["hive2-query"],
}

TEMPLATE_ENV = {**CONFIG, **JOB_PROPS, "functions": functions, "task_map": TASK_MAP}

with models.DAG(
    "hive",
    schedule_interval=None,  # Change to suit your needs
    start_date=dates.days_ago(0),  # Change to suit your needs
    user_defined_macros=TEMPLATE_ENV,
) as dag:

    hive_script = hive.HiveOperator(
        task_id="hive-script",
        trigger_rule="one_success",
        hql="script.q",
        hive_cli_conn_id='"hive_local"',
        mapred_queue="default",
        hiveconfs=PropertySet(config=CONFIG, job_properties=JOB_PROPS).xml_escaped.merged,
        mapred_job_name="hive-script",
    )

    hive_query_prepare = bash.BashOperator(
        task_id="hive-query_prepare",
        trigger_rule="one_success",
        bash_command="hadoop fs  -mkdir  %s ;"
        % (
            shlex.quote(
                "/{{functions.wf.user()}}/{{examplesRoot}}/apps/hive/output-data/ /{{functions.wf.user()}}/{{examplesRoot}}/apps/hive/output2-data/"
            )
        )
        + "hadoop fs  -rm -r -f  %s ;"
        % (shlex.quote("/{{functions.wf.user()}}/{{examplesRoot}}/apps/hive/input-data/")),
        params=PropertySet(config=CONFIG, job_properties=JOB_PROPS).merged,
    )

    hive_query = hive.HiveOperator(
        task_id="hive-query",
        trigger_rule="one_success",
        hql="DROP TABLE IF EXISTS test_query;\n CREATE EXTERNAL TABLE test_query (a INT) STORED AS TEXTFILE LOCATION '/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/hive/input-data/';\n INSERT OVERWRITE DIRECTORY '/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/hive/output-data/' SELECT * FROM test_query;",
        hive_cli_conn_id='"hive_local"',
        mapred_queue="default",
        hiveconfs=PropertySet(config=CONFIG, job_properties=JOB_PROPS).xml_escaped.merged,
        mapred_job_name="hive-query",
    )

    hive_query_prepare >> hive_query

    hive2_script = hive.HiveOperator(
        task_id="hive2-script",
        trigger_rule="one_success",
        hql="script.q",
        hive_cli_conn_id='"hive_local"',
        mapred_queue="default",
        hiveconfs=PropertySet(config=CONFIG, job_properties=JOB_PROPS).xml_escaped.merged,
        mapred_job_name="hive2-script",
    )

    hive2_query = hive.HiveOperator(
        task_id="hive2-query",
        trigger_rule="one_success",
        hql="DROP TABLE IF EXISTS test_query;\n CREATE EXTERNAL TABLE test_query (a INT) STORED AS TEXTFILE LOCATION '/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/hive/input-data/';\n INSERT OVERWRITE DIRECTORY '/user/{{functions.wf.user()}}/{{examplesRoot}}/apps/hive/output-data/' SELECT * FROM test_query;",
        hive_cli_conn_id='"hive_local"',
        mapred_queue="default",
        hiveconfs=PropertySet(config=CONFIG, job_properties=JOB_PROPS).xml_escaped.merged,
        mapred_job_name="hive2-query",
    )

    hive2_script >> hive2_query
    hive_query >> hive2_script
    hive_script >> hive_query_prepare
