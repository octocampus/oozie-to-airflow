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
"""Tests shell mapper"""
import ast
import unittest
from xml.etree import ElementTree as ET


from o2a.mappers import shell_mapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.tasks.shell.shell_local_task import ShellLocalTask


class TestShellMapper(unittest.TestCase):
    def setUp(self):
        # language=XML
        shell_node_str = """
        <shell>
            <resource-manager>localhost:8032</resource-manager>
            <name-node>hdfs://localhost:8020</name-node>
            <prepare>
                <delete path="${nameNode}/examples/output-data/demo/pig-node" />
                <delete path="${nameNode}/examples/output-data/demo/pig-node2" />
                <mkdir path="${nameNode}/examples/input-data/demo/pig-node" />
                <mkdir path="${nameNode}/examples/input-data/demo/pig-node2" />
             </prepare>
             <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>${queueName}</value>
                </property><property>
                    <name>mapred.map.output.compress</name>
                    <value>false</value>
                </property>
            </configuration>
            <exec>echo</exec>
            <argument>arg1</argument>
            <argument>$VAR2</argument>
            <env-var>VAR1=value1</env-var>
            <env-var>VAR2=value2</env-var>
            <file>${scriptpath}#${scriptalias}</file>
            <archive>${archivepath}#${archivealias}</archive>

        </shell>
        """
        self.shell_node = ET.fromstring(shell_node_str)

    def test_create_mapper_no_jinja(self):
        mapper = self._get_shell_mapper(job_properties={}, config={})
        mapper.on_parse_node()
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(self.shell_node, mapper.oozie_node)
        self.assertEqual("localhost:8032", mapper.resource_manager)
        self.assertEqual("hdfs://localhost:8020", mapper.name_node)
        self.assertEqual("{{queueName}}", mapper.props.action_node_properties["mapred.job.queue.name"])
        self.assertEqual("echo arg1 $VAR2", mapper.bash_command)

    def test_create_mapper_jinja(self):
        # test jinja templating
        self.shell_node.find("resource-manager").text = "${resourceManager}"
        self.shell_node.find("name-node").text = "${nameNode}"
        job_properties = {
            "resourceManager": "localhost:9999",
            "nameNode": "hdfs://localhost:8021",
            "queueName": "myQueue",
            "examplesRoot": "examples",
        }
        config = {"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"}

        mapper = self._get_shell_mapper(job_properties=job_properties, config=config)
        mapper.on_parse_node()

        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(self.shell_node, mapper.oozie_node)
        self.assertEqual("{{resourceManager}}", mapper.resource_manager)
        self.assertEqual("{{nameNode}}", mapper.name_node)
        self.assertEqual("{{queueName}}", mapper.props.action_node_properties["mapred.job.queue.name"])
        self.assertEqual("echo arg1 $VAR2", mapper.bash_command)

    def test_to_tasks_and_relations(self):
        job_properties = {"nameNode": "hdfs://localhost:9020/", "queueName": "default"}
        config = {}
        mapper = self._get_shell_mapper(job_properties=job_properties, config=config)
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            [
                ShellLocalTask(
                    task_id="test_id",
                    template_name="shell/shell.tpl",
                    trigger_rule="one_success",
                    template_params={
                        "bash_command": "echo arg1 $VAR2",
                        "env": {
                            "VAR1": "value1",
                            "VAR2": "value2",
                        },
                        "prepare": [
                            ("delete", "{{nameNode}}/examples/output-data/demo/pig-node"),
                            ("delete", "{{nameNode}}/examples/output-data/demo/pig-node2"),
                            ("mkdir", "{{nameNode}}/examples/input-data/demo/pig-node"),
                            ("mkdir", "{{nameNode}}/examples/input-data/demo/pig-node2"),
                        ],
                        "files": ["{{scriptpath}}#{{scriptalias}}"],
                        "archives": ["{{archivepath}}#{{archivealias}}"],
                    },
                ),
            ],
            tasks,
        )
        self.assertEqual(relations, [])

    def test_required_imports(self):
        job_properties = {"nameNode": "hdfs://localhost:9020/"}
        mapper = self._get_shell_mapper(job_properties=job_properties, config={})
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_shell_mapper(self, job_properties, config):
        return shell_mapper.ShellMapper(
            oozie_node=self.shell_node,
            name="test_id",
            dag_name="DAG_NAME_B",
            props=PropertySet(job_properties=job_properties, config=config),
            input_directory_path="/tmp/input-directory-path/",
        )

    def test_get_env_vars(self):
        mapper = self._get_shell_mapper(job_properties={}, config={})
        env_vars = mapper.get_env_vars()

        expected_env_vars = {
            "VAR1": "value1",
            "VAR2": "value2",
        }

        self.assertEqual(expected_env_vars, env_vars)
