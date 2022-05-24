"""Tests coordinator xml parser"""
import datetime
from os import path
from typing import Dict, List, NamedTuple, Optional

import unittest
from unittest import mock
from xml.etree import ElementTree as ET

from parameterized import parameterized

from o2a.converter import workflow_xml_parser
from o2a.converter.mappers import ACTION_MAP
from o2a.converter.workflow import Workflow

from o2a.definitions import EXAMPLE_DEMO_PATH, EXAMPLES_PATH
from o2a.converter.Coordinator import Coordinator
from o2a.mappers import dummy_mapper, pig_mapper
from o2a.mappers import ssh_mapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.converter import coordinator_xml_parser
from dateutil.parser import parse as date_parse


class TestCoordinatorXmlParser(unittest.TestCase):
    def setUp(self):
        props = PropertySet(job_properties={
            "env": "${env}",
            "year": "2022",
            "start": "${year}-05-03T20:56:35.450686Z",
            "end": "${year}-12-03T20:56:35.450686Z"
        }, config={})

        coordinator = Coordinator(
            input_directory_path=EXAMPLE_DEMO_PATH
        )
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

    @parameterized.expand([
        ("<controls></controls>", -1),
        ("<controls><timeout>10</timeout></controls>", 10),
        ("<controls><timeout>-1</timeout></controls>", -1),
        ("<controls><timeout></timeout></controls>", -1)

    ])
    def test_parse_timeout(self, controls_node_str, expected):
        controls_node = ET.fromstring(controls_node_str)
        self.parser.parse_timeout(controls_node)

        actual = self.parser.coordinator.timeout

        self.assertEqual(expected, actual)

    @parameterized.expand([
        ("<controls></controls>", 1),
        ("<controls><concurrency>10</concurrency></controls>", 10),
        ("<controls><concurrency>-1</concurrency></controls>", 1),
        ("<controls><concurrency></concurrency></controls>", 1)

    ])
    def test_parse_concurrency(self, controls_node_str: ET.Element, expected: int):
        controls_node = ET.fromstring(controls_node_str)
        self.parser.parse_concurrency(controls_node)

        actual = self.parser.coordinator.concurrency

        self.assertEqual(expected, actual)

    @parameterized.expand([
        ("<controls></controls>", "FIFO"),
        ("<controls><execution></execution></controls>", "FIFO"),
        ("<controls><execution>FIFO</execution></controls>", "FIFO"),
        ("<controls><execution>LIFO</execution></controls>", "LIFO"),
        ("<controls><execution>NONE</execution></controls>", "NONE")

    ])
    def test_parse_execution(self, controls_node_str: ET.Element, expected: int):
        controls_node = ET.fromstring(controls_node_str)
        self.parser.parse_execution(controls_node)

        actual = self.parser.coordinator.execution

        self.assertEqual(expected, actual)

    @parameterized.expand([
        ("<controls></controls>", 12),
        ("<controls><throttle></throttle></controls>", 12),
        ("<controls><throttle>4</throttle></controls>", 4),
        ("<controls><throttle>1</throttle></controls>", 1),

    ])
    def test_parse_throttle(self, controls_node_str: ET.Element, expected: int):
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
        pass

    def parse_dataset_node(self):
        pass