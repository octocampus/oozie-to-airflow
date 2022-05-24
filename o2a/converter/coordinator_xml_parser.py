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
"""Parsing module """
import datetime
import logging
import os
import uuid

# noinspection PyPep8Naming
import xml.etree.ElementTree as ET

# noinspection PyPackageRequirements
from typing import Dict, List, Type, Optional

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.constants import HDFS_FOLDER
from o2a.converter.exceptions import ParseException
from o2a.converter.oozie_node import OozieActionNode, OozieControlNode
from o2a.converter.renderers import BaseRenderer
from o2a.converter.Coordinator import Coordinator
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.base_mapper import BaseMapper
from o2a.mappers.decision_mapper import DecisionMapper
from o2a.mappers.dummy_mapper import DummyMapper
from o2a.mappers.end_mapper import EndMapper
from o2a.mappers.fork_mapper import ForkMapper
from o2a.mappers.join_mapper import JoinMapper
from o2a.mappers.kill_mapper import KillMapper
from o2a.mappers.start_mapper import StartMapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.transformers.base_transformer import BaseWorkflowTransformer
from o2a.utils import xml_utils
from  dateutil.parser import parse as date_parse
from o2a.o2a_libs import el_parser


COORDINATOR_FOLDER = "coordinator"
COORD_START_ATTRIB = "start"
COORD_END_ATTRIB = "end"
COORD_TIMEZONE_ATTRIB = "timezone"
COORD_NAME_ATTRIB = "name"
COORD_FREQ_ATTRIB = "frequency"

# noinspection PyDefaultArgument
class CoordinatorXmlParser:
    """Parses XML of an Oozie Coordinator"""

    def __init__(
            self,

            props: PropertySet,
            action_mapper: Dict[str, Type[ActionMapper]],
            renderer: BaseRenderer,
            coordinator: Coordinator,
            transformers: List[BaseWorkflowTransformer] = None,
    ):
        self.coordinator = coordinator
        self.coordinator_file = os.path.join(coordinator.input_directory_path, COORDINATOR_FOLDER, "coord.xml")
        self.props = props
        self.action_map = action_mapper
        self.renderer = renderer
        self.transformers = transformers

        self.coordinator.root_node = self.get_root_node()


    def parse_start_attribute(self) -> str:
        """
        Parses coordinator start date attribute
        It
        """

        start_date = self.coordinator.root_node.attrib[COORD_START_ATTRIB]
        self.coordinator.start = el_parser.translate(start_date)

    def parse_name_attribute(self) -> str:
        """
        Parses coordinator name  attribute
        It
        """

        name = self.coordinator.root_node.attrib[COORD_NAME_ATTRIB]
        self.coordinator.name = el_parser.translate(name)

    def parse_end_attribute(self) -> str:
        """
        Parses coordinator end attribute
        """

        end_date = self.coordinator.root_node.attrib[COORD_END_ATTRIB]
        self.coordinator.end  = el_parser.translate(end_date)

    def parse_timezone_attribute(self) -> str:
        """Parses coordinator timezone attribute"""

        timezone = self.coordinator.root_node.attrib[COORD_TIMEZONE_ATTRIB]
        self.coordinator.timezone = el_parser.translate(timezone)

    def parse_coordinator_frequency(self):
        frequency = self.coordinator.root_node.attrib[COORD_FREQ_ATTRIB]
        self.coordinator.frequency = el_parser.translate(frequency)

    def parse_node(self, root, node):
        """
        Given a node, determines its tag, and then passes it to the correct
        parser.

        :param root:  The root node of the XML tree.
        :param node: The node to parse.
        """
        if "controls" in node.tag:
            self.parse_controls_node(self, node)

    def parse_controls_node(self, controls_node):
        self.parse_timeout(self, controls_node)
        self.parse_concurrency(self, controls_node)
        self.parse_execution(self, controls_node)
        self.pars_throttle(self, controls_node)

    def parse_datasets_node(self, datasets_node: ET.Element):

        for dataset_node in datasets_node.iter():
            # Strip namespaces
            dataset_node.tag = dataset_node.tag
            self.parse_dataset_node(dataset_node)

    def parse_dataset_node(self, dataset_node):
        name = self.parse_dataset_name_(dataset_node)
        frequency = self.parse_dataset_frequency(dataset_node)
        initial_instance = self.parse_dataset_initial_instance(dataset_node)
        timezone = self.parse_dataset_timezone(dataset_node)
        uri_template = self.parse_dataset_uri_template(dataset_node)
        done_flag = self.parse_done_flag(dataset_node)



    def parse_coordinator(self):
        """Parses coordinator replacing invalid characters in the names of the nodes"""
        tree = ET.parse(self.coordinator_file)
        root = tree.getroot()
        for node in tree.iter():
            # Strip namespaces
            node.tag = node.tag.split("}")[1][0:]

        logging.info("Stripped namespaces, and replaced invalid characters.")

        for node in root:
            logging.debug(f"Parsing node: {node}")
            self.parse_node(root, node)

    def get_root_node(self):
        tree = ET.parse(self.coordinator_file)
        return tree.getroot()

    def parse_timeout(self, controls_node: ET.Element):
        timeout = int(xml_utils.get_tag_el_text(controls_node, "timeout", "-1"))
        self.coordinator.timeout = timeout

    def parse_concurrency(self, controls_node: ET.Element):
        concurrency = int(xml_utils.get_tag_el_text(controls_node, "concurrency", "1"))
        if concurrency < 1:
            concurrency = 1
        self.coordinator.concurrency = concurrency

    def parse_execution(self, controls_node: ET.Element):
        execution = xml_utils.get_tag_el_text(controls_node, "execution", "FIFO")

        self.coordinator.execution = execution

    def parse_throttle(self, controls_node):
        throttle = int(xml_utils.get_tag_el_text(controls_node, "throttle", "12"))
        if throttle < 0:
            throttle = 12
        self.coordinator.throttle = throttle

    @staticmethod
    def parse_dataset_name(dataset_node):
        return dataset_node.attrib["name"]

    @staticmethod
    def parse_dataset_frequency(dataset_node):
        frequency = dataset_node.attrib["frequency"]
        return el_parser.translate(frequency)

    @staticmethod
    def parse_dataset_initial_instance(dataset_node):
        initial_instance = dataset_node.attrib["initial-instance"]
        return el_parser.translate(initial_instance)

    @staticmethod
    def parse_dataset_timezone(self, dataset_node):
        timezone = dataset_node.attrib["timezone"]
        return el_parser.translate(timezone)

    def parse_dataset_uri_template(self, dataset_node):


    def parse_done_flag(self, dataset_node):
        pass
