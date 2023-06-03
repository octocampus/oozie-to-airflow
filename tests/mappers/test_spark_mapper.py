# -*- coding: utf-8 -*-
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests Spark Mapper"""
import ast
import unittest
from xml.etree import ElementTree as ET


from o2a.mappers import spark_mapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.tasks.spark_local_task import SparkLocalTask

EXAMPLE_JOB_PROPS = {"nameNode": "hdfs://", "userName": "test_user", "examplesRoot": "examples"}

EXAMPLE_CONFIG = {
    "dataproc_cluster": "my-cluster",
    "gcp_region": "europe-west3",
    "spark_cli_conn_id": "spark_default",
}

# language=XML
EXAMPLE_XML_WITH_PREPARE = """
<spark name="decision">
    <job-tracker>foo:8021</job-tracker>
    <name-node>bar:8020</name-node>
    <prepare>
        <mkdir path="hdfs:///tmp/mk_path" />
        <delete path="hdfs:///tmp/d_path" />
    </prepare>
    <configuration>
        <property>
            <name>mapred.compress.map.output</name>
            <value>true</value>
        </property>
    </configuration>
    <master>local[*]</master>
    <name>Spark Examples</name>
    <mode>client</mode>
    <class>org.apache.spark.examples.mllib.JavaALS</class>
    <jar>/lib/spark-examples_2.10-1.1.0.jar</jar>
    <spark-opts>--executor-memory 20G --num-executors 50
 --conf spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"</spark-opts>
    <arg>inputpath=hdfs:///input/file.txt</arg>
    <arg>value=2</arg>
</spark>"""

# language=XML
EXAMPLE_XML_WITHOUT_PREPARE = """
<spark name="decision">
    <job-tracker>foo:8021</job-tracker>
    <name-node>bar:8020</name-node>
    <configuration>
        <property>
            <name>mapred.compress.map.output</name>
            <value>true</value>
        </property>
    </configuration>
    <master>local[*]</master>
    <name>Spark Examples</name>
    <mode>client</mode>
    <class>org.apache.spark.examples.mllib.JavaALS</class>
    <jar>/user/${userName}/${examplesRoot}/apps/spark/lib/oozie-examples-4.3.0.jar</jar>
    <spark-opts>
    --executor-memory 20G --num-executors 50
    --conf spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"
    </spark-opts>
    <arg>inputpath=hdfs:///input/file.txt</arg>
    <arg>value=2</arg>
    <arg>/user/${userName}/${examplesRoot}/apps/spark/lib/oozie-examples-4.3.0.jar</arg>
</spark>
"""


class TestSparkMapperWithPrepare(unittest.TestCase):
    def test_create_mapper(self):
        spark_node = ET.fromstring(EXAMPLE_XML_WITH_PREPARE)
        mapper = self._get_spark_mapper(spark_node)
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(spark_node, mapper.oozie_node)

    def test_to_tasks_and_relations_with_prepare_node(self):
        spark_node = ET.fromstring(EXAMPLE_XML_WITH_PREPARE)
        mapper = self._get_spark_mapper(spark_node)
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            [
                SparkLocalTask(
                    task_id="test_id",
                    template_name="spark/spark.tpl",
                    trigger_rule="all_success",
                    template_params={
                        "conf": {
                            "mapred.compress.map.output": "true",
                            "spark.executor.extraJavaOptions": "-XX:+HeapDumpOnOutOfMemoryError "
                            "-XX:HeapDumpPath=/tmp",
                        },
                        "name": "Spark Examples",
                        "java_class": "org.apache.spark.examples.mllib.JavaALS",
                        "spark_conn_id": "spark_default_conn_id",
                        "jars": "/lib/spark-examples_2.10-1.1.0.jar",
                        "application_args": ["inputpath=hdfs:///input/file.txt", "value=2"],
                        "oozie_files": [],
                        "oozie_archives": [],
                        "files": None,
                        "archives": None,
                        "queue": None,
                        "master": "local[*]",
                        "mode": "client",
                        "executor_memory": "20G",
                        "num_executors": "50",
                        "driver_memory": None,
                        "executor_cores": None,
                        "total_executor_cores": None,
                        "prepare": [("mkdir", "hdfs:///tmp/mk_path"), ("delete", "hdfs:///tmp/d_path")],
                    },
                ),
            ],
            tasks,
        )

    def test_to_tasks_and_relations_without_prepare_node(self):
        spark_node = ET.fromstring(EXAMPLE_XML_WITHOUT_PREPARE)
        mapper = self._get_spark_mapper(spark_node)
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            [
                SparkLocalTask(
                    task_id="test_id",
                    template_name="spark/spark.tpl",
                    trigger_rule="all_success",
                    template_params={
                        "conf": {
                            "mapred.compress.map.output": "true",
                            "spark.executor.extraJavaOptions": "-XX:+HeapDumpOnOutOfMemoryError "
                            "-XX:HeapDumpPath=/tmp",
                        },
                        "name": "Spark Examples",
                        "java_class": "org.apache.spark.examples.mllib.JavaALS",
                        "spark_conn_id": "spark_default_conn_id",
                        "jars": "/user/{{userName}}/{{examplesRoot}}/apps/spark/lib/oozie-examples-4.3.0.jar",
                        "application_args": [
                            "inputpath=hdfs:///input/file.txt",
                            "value=2",
                            "/user/{{userName}}/{{examplesRoot}}/apps/spark/lib/oozie-examples-4.3.0.jar",
                        ],
                        "oozie_files": [],
                        "oozie_archives": [],
                        "files": None,
                        "archives": None,
                        "queue": None,
                        "master": "local[*]",
                        "mode": "client",
                        "executor_memory": "20G",
                        "num_executors": "50",
                        "driver_memory": None,
                        "executor_cores": None,
                        "total_executor_cores": None,
                        "prepare": [],
                    },
                )
            ],
            tasks,
        )
        self.assertEqual(relations, [])

    def test_required_imports(self):
        spark_node = ET.fromstring(EXAMPLE_XML_WITHOUT_PREPARE)
        mapper = self._get_spark_mapper(spark_node)
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    @staticmethod
    def _get_spark_mapper(spark_node):
        mapper = spark_mapper.SparkMapper(
            oozie_node=spark_node,
            name="test_id",
            dag_name="DAG_NAME_B",
            props=PropertySet(job_properties=EXAMPLE_JOB_PROPS, config=EXAMPLE_CONFIG),
            input_directory_path="/tmp/input-directory-path/",
        )
        return mapper
