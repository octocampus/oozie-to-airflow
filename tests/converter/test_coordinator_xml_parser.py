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

"""Tests coordinator xml parser"""

import unittest
from unittest import mock
from xml.etree import ElementTree as ET

from parameterized import parameterized

from o2a.converter.data_events import InputEvent, OutputEvent
from o2a.converter.dataset import Dataset
from o2a.converter.mappers import ACTION_MAP

from o2a.definitions import EXAMPLE_DEMO_PATH
from o2a.converter.coordinator import Coordinator
from o2a.o2a_libs.property_utils import PropertySet
from o2a.converter import coordinator_xml_parser


class TestCoordinatorXmlParser(unittest.TestCase):
    def setUp(self):
        props = PropertySet(
            job_properties={
                "env": "${env}",
                "year": "2022",
                "start": "${year}-05-03T20:56:35.450686Z",
                "end": "${year}-12-03T20:56:35.450686Z",
            },
            config={},
        )

        coordinator = Coordinator(input_directory_path=EXAMPLE_DEMO_PATH)
        self.parser = coordinator_xml_parser.CoordinatorXmlParser(
            coordinator=coordinator, props=props, action_mapper=ACTION_MAP, renderer=mock.MagicMock()
        )

    def test_parse_start_attribute(self):
        expected_start_date = "{{start}}"
        self.parser.parse_start_attribute()
        actual_start_date = self.parser.coordinator.start

        self.assertEqual(expected_start_date, actual_start_date)

    def test_parse_end_attribute(self):
        self.parser.parse_end_attribute()

        expected_end_date = "{{end}}"
        actual_end_date = self.parser.coordinator.end

        self.assertEqual(expected_end_date, actual_end_date)

    def test_parse_timezone_attribute(self):
        self.parser.parse_timezone_attribute()

        expected_timezone = "{{timezone}}"
        actual_timezone = self.parser.coordinator.timezone

        self.assertEqual(expected_timezone, actual_timezone)

    @parameterized.expand(
        [
            ("<controls></controls>", "-1"),
            ("<controls><timeout>10</timeout></controls>", "10"),
            ("<controls><timeout>-1</timeout></controls>", "-1"),
            ("<controls><timeout></timeout></controls>", "-1"),
        ]
    )
    def test_parse_timeout(self, controls_node_str, expected):
        controls_node = ET.fromstring(controls_node_str)
        self.parser.parse_timeout(controls_node)

        actual = self.parser.coordinator.timeout

        self.assertEqual(expected, actual)

    @parameterized.expand(
        [
            ("<controls></controls>", "1"),
            ("<controls><concurrency>10</concurrency></controls>", "10"),
            ("<controls><concurrency>-1</concurrency></controls>", "-1"),
            ("<controls><concurrency></concurrency></controls>", "1"),
        ]
    )
    def test_parse_concurrency(self, controls_node_str: str, expected: str):
        controls_node = ET.fromstring(controls_node_str)
        self.parser.parse_concurrency(controls_node)

        actual = self.parser.coordinator.concurrency

        self.assertEqual(expected, actual)

    @parameterized.expand(
        [
            ("<controls></controls>", "FIFO"),
            ("<controls><execution></execution></controls>", "FIFO"),
            ("<controls><execution>FIFO</execution></controls>", "FIFO"),
            ("<controls><execution>LIFO</execution></controls>", "LIFO"),
            ("<controls><execution>NONE</execution></controls>", "NONE"),
        ]
    )
    def test_parse_execution(self, controls_node_str: str, expected: str):
        controls_node = ET.fromstring(controls_node_str)
        self.parser.parse_execution(controls_node)

        actual = self.parser.coordinator.execution

        self.assertEqual(expected, actual)

    @parameterized.expand(
        [
            ("<controls></controls>", 12),
            ("<controls><throttle></throttle></controls>", 12),
            ("<controls><throttle>4</throttle></controls>", 4),
            ("<controls><throttle>1</throttle></controls>", 1),
        ]
    )
    def test_parse_throttle(self, controls_node_str: str, expected: int):
        controls_node = ET.fromstring(controls_node_str)
        self.parser.parse_throttle(controls_node)

        actual = self.parser.coordinator.throttle

        self.assertEqual(expected, actual)

    def test_parse_name_attribute(self):
        self.parser.parse_name_attribute()

        expected = "coord_{{env}}_purge_daily"
        actual = self.parser.coordinator.name
        self.assertEqual(expected, actual)

    def test_parse_datasets_nodes(self):
        datasets_node_str = """
<datasets>
    <dataset name="din" frequency="${coord:endOfDays(1)}"
        initial-instance="2009-01-02T08:00Z" timezone="America/Los_Angeles" >

         <uri-template>${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}</uri-template>
    </dataset>
    <dataset name="dout" frequency="${coord:minutes(30)}" initial-instance="2009-01-02T08:00Z" timezone="UTC">
         <uri-template>${baseFsURI}/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}</uri-template>
         <done-flag></done-flag>
    </dataset>
</datasets>
        """
        datasets_node = ET.fromstring(datasets_node_str)

        expected = [
            Dataset(
                name="din",
                frequency="59 23 */1 * *",
                initial_instance="2009-01-02T08:00Z",
                timezone="America/Los_Angeles",
                uri_template="{{baseFsURI}}/{{YEAR}}/{{MONTH}}/{{DAY}}/{{HOUR}}/{{MINUTE}}",
                done_flag={"done_flag_node": False, "done_flag_el_text": None},
            ),
            Dataset(
                name="dout",
                frequency="*/30 * * * *",
                initial_instance="2009-01-02T08:00Z",
                timezone="UTC",
                uri_template="{{baseFsURI}}/{{YEAR}}/{{MONTH}}/{{DAY}}/{{HOUR}}/{{MINUTE}}",
                done_flag={"done_flag_node": True, "done_flag_el_text": None},
            ),
        ]

        self.parser.parse_datasets_node(datasets_node)
        actual = self.parser.coordinator.datasets

        self.assertEqual(expected, actual)

    def test_parse_dataset_node(self):
        pass

    def test_parse_input_events(self):

        input_event_str = """
<input-events>
    <data-in name="input" dataset="logs">
        <instance>2009-01-02T08:00Z</instance>
    </data-in>
     <data-in name="input" dataset="logs">
          <start-instance>${coord:current(-6)}</start-instance>
          <end-instance>${coord:current(0)}</end-instance>
    </data-in>
</input-events>
        """

        input_event_node = ET.fromstring(input_event_str)

        self.parser.parse_input_events(input_event_node)

        expected = [
            InputEvent(
                name="input",
                dataset="logs",
                instance="2009-01-02T08:00Z",
                start_instance=None,
                end_instance=None,
            ),
            InputEvent(
                name="input",
                dataset="logs",
                instance=None,
                start_instance="{{functions.coord.current(-6)}}",
                end_instance="{{functions.coord.current(0)}}",
            ),
        ]

        actual = self.parser.coordinator.input_events

        self.assertEquals(expected, actual)

    def test_parse_output_events(self):

        output_event_str = """
 <output-events>
    <data-out name="output" dataset="siteAccessStats">
        <instance>${coord:current(0)}</instance>
    </data-out>
    <data-in name="input" dataset="logs">
        <start-instance>${coord:current(-23)}</start-instance>
        <end-instance>${coord:current(0)}</end-instance>
    </data-in>
</output-events>
        """

        output_event_node = ET.fromstring(output_event_str)

        self.parser.parse_output_events(output_event_node)

        expected = [
            OutputEvent(
                name="output",
                dataset="siteAccessStats",
                instance="{{functions.coord.current(0)}}",
                nocleanup=False,
            ),
            OutputEvent(name="input", dataset="logs", instance=None, nocleanup=False),
        ]

        actual = self.parser.coordinator.output_events

        self.assertEquals(expected, actual)

    def test_parse_action_node(self):
        self.test_parse_input_events()

    def test_parse_workflow_node(self):
        action_node_str = """
<action>
 <workflow>
  <app-path>hdfs://bar:8020/usr/joe/logsprocessor-wf</app-path>
  <configuration>
    <property>
      <name>wfInput</name>
      <value>${coord:dataIn('input')}</value>
    </property>
    <property>
      <name>wfOutput</name>
      <value>${coord:dataOut('output')}</value>
    </property>
 </configuration>
</workflow>
</action>
        """

        action_node = ET.fromstring(action_node_str)

        expected = {
            "app_path": "hdfs://bar:8020/usr/joe/logsprocessor-wf",
            "configuration": {
                "wfInput": "{{functions.coord.data_in('input')}}",
                "wfOutput": "{{functions.coord.data_out('output')}}",
            },
        }
        actual = self.parser.parse_workflow_node(action_node)

        self.assertEqual(expected, actual)
