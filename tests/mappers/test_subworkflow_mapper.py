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
"""Tests for subworkflow mapper"""
import ast
import os
from contextlib import suppress
from unittest import mock, TestCase
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.mappers import ACTION_MAP
from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.definitions import EXAMPLE_SUBWORKFLOW_PATH
from o2a.mappers import subworkflow_mapper
from o2a.o2a_libs.property_utils import PropertySet


class TestSubworkflowMapper(TestCase):

    subworkflow_properties = {
        "nameNode": "hdfs://",
        "oozie.wf.application.path": "hdfs:///user/pig/examples/pig",
    }

    main_properties = {
        "oozie.wf.application.path": "hdfs:///user/pig/examples/pig",
        "examplesRoot": "examples",
        "nameNode": "hdfs://",
        "resourceManager": "localhost:8032",
    }

    config = {
        "dataproc_cluster": "test_cluster",
        "gcp_conn_id": "google_cloud_default",
        "gcp_region": "europe-west3",
        "gcp_uri_prefix": "gs://test_bucket/dags",
    }

    SUBDAG_TEST_FILEPATH = os.path.join("/tmp", "pig", "subdag_pig.py")

    @classmethod
    def setUpClass(cls):
        subworkflow_node_str = """
<sub-workflow>
    <app-path>${nameNode}/user/${examplesRoot}/pig</app-path>
    <propagate-configuration />
    <configuration>
        <property>
            <name>resourceManager</name>
            <value>${resourceManager}</value>
        </property>
    </configuration>
</sub-workflow>"""
        subworkflow_node_no_propagate_str = """
<sub-workflow>
    <app-path>${nameNode}/user/${examplesRoot}/pig</app-path>
    <configuration>
        <property>
            <name>resourceManager</name>
            <value>${resourceManager}</value>
        </property>
    </configuration>
</sub-workflow>"""

        super().setUpClass()
        cls.subworkflow_node = ET.fromstring(subworkflow_node_str)
        cls.subworkflow_node_no_propagate = ET.fromstring(subworkflow_node_no_propagate_str)

    def tearDown(self) -> None:
        with suppress(OSError):
            os.remove(self.SUBDAG_TEST_FILEPATH)

    @mock.patch("o2a.utils.el_utils.extract_evaluate_properties")
    def test_create_mapper_jinja(self, parse_els):
        # Given

        parse_els.side_effect = [self.subworkflow_properties, self.config]
        # When
        mapper = self._get_subwf_mapper(self.subworkflow_node)

        # Then
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(self.subworkflow_node, mapper.oozie_node)
        self.assertEqual("examples", mapper.props.merged["examplesRoot"])
        self.assertEqual("hdfs://", mapper.props.merged["nameNode"])
        self.assertEqual("hdfs:///user/pig/examples/pig", mapper.props.merged["oozie.wf.application.path"])
        self.assertEqual("localhost:8032", mapper.props.merged["resourceManager"])

    @mock.patch("o2a.o2a_libs.el_wf_functions.user", return_value="user")
    def test_create_mapper_jinja_no_propagate(self, parse_els):
        # Given
        parse_els.side_effect = [self.subworkflow_properties, self.config]
        self.assertFalse(os.path.isfile(self.SUBDAG_TEST_FILEPATH))

        # When
        mapper = self._get_subwf_mapper(self.subworkflow_node_no_propagate)

        # Then
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(self.subworkflow_node_no_propagate, mapper.oozie_node)
        self.assertEqual(self.main_properties, mapper.props.job_properties)
        # Propagate config node is missing, should NOT forward config job_properties
        self.assertEqual(PropertySet(config={}, job_properties={}), mapper.get_child_props())

    @mock.patch("o2a.utils.el_utils.extract_evaluate_properties")
    def test_to_tasks_and_relations(self, parse_els):
        # Given
        parse_els.side_effect = [self.subworkflow_properties, self.config]
        mapper = self._get_subwf_mapper(self.subworkflow_node)
        # When
        tasks, relations = mapper.to_tasks_and_relations()

        # Then
        self.assertEqual(
            [
                Task(
                    task_id="test_id",
                    template_name="subwf.tpl",
                    template_params={
                        "app_name": "test_id",
                        "propagate": True,
                        "override_subwf_config": False,
                    },
                ),
                Task(
                    task_id="test_id_state",
                    template_name="subwf_state.tpl",
                    template_params={"taskgroup": "test_id"},
                    trigger_rule=TriggerRule.ALL_DONE,
                ),
            ],
            tasks,
        )
        self.assertEqual([Relation(from_task_id="test_id", to_task_id="test_id_state")], relations)

    def test_required_imports(self):
        mapper = self._get_subwf_mapper(self.subworkflow_node)
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_subwf_mapper(self, oozie_node):
        return subworkflow_mapper.SubworkflowMapper(
            input_directory_path=EXAMPLE_SUBWORKFLOW_PATH,
            output_directory_path="/tmp",
            oozie_node=oozie_node,
            name="test_id",
            dag_name="test",
            action_mapper=ACTION_MAP,
            props=PropertySet(job_properties=self.main_properties, config=self.config),
            renderer=mock.MagicMock(),
            subdag_folder="/tmp",
        )
