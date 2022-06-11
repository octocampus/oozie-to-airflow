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
import logging
import os
import re

# noinspection PyPep8Naming
import xml.etree.ElementTree as ET

# noinspection PyPackageRequirements
from typing import Dict, List, Type

from o2a.converter.data_events import InputEvent, OutputEvent
from o2a.converter.dataset import Dataset

from o2a.converter.renderers import BaseRenderer
from o2a.converter.Coordinator import Coordinator
from o2a.mappers.action_mapper import ActionMapper

from o2a.o2a_libs.property_utils import PropertySet
from o2a.transformers.base_transformer import BaseWorkflowTransformer
from o2a.utils import xml_utils, el_utils
from o2a.o2a_libs import el_parser
from o2a.utils.config_extractors import extract_properties_from_configuration_node

COORDINATOR_FOLDER = "coordinator"
COORD_START_ATTRIB = "start"
COORD_END_ATTRIB = "end"
COORD_TIMEZONE_ATTRIB = "timezone"
COORD_NAME_ATTRIB = "name"
COORD_FREQ_ATTRIB = "frequency"
DATASET_DONE_FLAG = "done-flag"


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
        self.coordinator: Coordinator = coordinator
        self.coordinator_file = os.path.join(
            coordinator.input_directory_path,
            COORDINATOR_FOLDER,
            "coord.xml"
        )
        self.props = props
        self.action_map = action_mapper
        self.renderer = renderer
        self.transformers = transformers

        self.coordinator.root_node = self.get_root_node()

    def parse_start_attribute(self):
        """
        Parses coordinator start date attribute
        It
        """

        start_date = self.coordinator.root_node.attrib[COORD_START_ATTRIB]
        self.coordinator.start = el_parser.translate(start_date)

    def parse_name_attribute(self):
        """
        Parses coordinator name  attribute
        It
        """

        name = self.coordinator.root_node.attrib[COORD_NAME_ATTRIB]
        self.coordinator.name = el_parser.translate(name)

    def parse_end_attribute(self):
        """
        Parses coordinator end attribute
        """

        end_date = self.coordinator.root_node.attrib[COORD_END_ATTRIB]
        self.coordinator.end = el_parser.translate(end_date)

    def parse_timezone_attribute(self):
        """Parses coordinator timezone attribute"""

        timezone = self.coordinator.root_node.attrib[COORD_TIMEZONE_ATTRIB]
        self.coordinator.timezone = el_parser.translate(timezone)

    def parse_coordinator_frequency(self):
        frequency = self.coordinator.root_node.attrib[COORD_FREQ_ATTRIB]

        el_frequency = el_parser.translate(frequency)
        self.coordinator.frequency = el_frequency

    def parse_node(self, node):
        """
        Given a node, determines its tag, and then passes it to the correct
        parser.

        :param node: The node to parse.
        """
        if "controls" in node.tag:
            self.parse_controls_node(node)
        if "datasets" in node.tag:
            self.parse_datasets_node(node)
        if "input-events" in node.tag:
            self.parse_input_events(node)
        if "output-events" in node.tag:
            self.parse_input_events(node)
        if "action" in node.tag:
            self.parse_action_node(node)

    def parse_controls_node(self, controls_node):
        self.parse_timeout(controls_node)
        self.parse_concurrency(controls_node)
        self.parse_execution(controls_node)
        self.parse_throttle(controls_node)

    def parse_datasets_node(self, datasets_node: ET.Element):
        datasets = []
        for dataset_node in datasets_node:
            parsed_dataset = self.parse_dataset_node(dataset_node)
            datasets.append(parsed_dataset)

        if datasets:
            self.coordinator.datasets = datasets

    def parse_dataset_node(self, dataset_node):
        name = self.parse_dataset_name(dataset_node)
        frequency = self.parse_dataset_frequency(dataset_node)
        initial_instance = self.parse_dataset_initial_instance(dataset_node)
        timezone = self.parse_dataset_timezone(dataset_node)
        uri_template = self.parse_dataset_uri_template(dataset_node)
        done_flag = self.parse_done_flag(dataset_node)

        return Dataset(
            name=name,
            frequency=frequency,
            initial_instance=initial_instance,
            timezone=timezone,
            uri_template=uri_template,
            done_flag=done_flag
        )

    def parse_coordinator(self):
        """Parses coordinator replacing invalid characters in the names of the nodes"""

        self.parse_coordinator_attributes()

        tree = ET.parse(self.coordinator_file)
        root = tree.getroot()
        for node in tree.iter():
            # Strip namespaces
            node.tag = node.tag.split("}")[1][0:]

        logging.info("Stripped namespaces, and replaced invalid characters.")

        for node in root:
            logging.debug(f"Parsing node: {node}")
            self.parse_node(node)

    def get_root_node(self):

        tree = ET.parse(self.coordinator_file)
        return tree.getroot()

    def parse_timeout(self, controls_node: ET.Element):
        timeout = xml_utils.get_tag_el_text(controls_node, "timeout", "-1")
        self.coordinator.timeout = timeout

    def parse_concurrency(self, controls_node: ET.Element):
        concurrency = xml_utils.get_tag_el_text(controls_node, "concurrency", "1")

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
        name = dataset_node.attrib["name"]
        return el_parser.translate(name)

    def parse_dataset_frequency(self, dataset_node):
        frequency = dataset_node.attrib["frequency"]
        return el_utils.resolve_job_properties_in_string(frequency, self.props)

    @staticmethod
    def parse_dataset_initial_instance(dataset_node):
        initial_instance = dataset_node.attrib["initial-instance"]
        return el_parser.translate(initial_instance)

    @staticmethod
    def parse_dataset_timezone(dataset_node):
        timezone = dataset_node.attrib["timezone"]
        return el_parser.translate(timezone)

    @staticmethod
    def parse_dataset_uri_template(dataset_node):
        return xml_utils.get_tag_el_text(dataset_node, 'uri-template')

    @staticmethod
    def parse_done_flag(dataset_node):
        done_flag_node = xml_utils.find_nodes_by_tag(dataset_node, DATASET_DONE_FLAG)

        done_flag_el_text = None
        done_flag = False
        if done_flag_node:
            done_flag_el_text = xml_utils.get_tag_el_text(dataset_node, DATASET_DONE_FLAG)
            done_flag = True
        return {
            "done_flag_node": done_flag,
            "done_flag_el_text": done_flag_el_text
        }

    def parse_input_events(self, input_events_node):
        parsed_nodes = []
        for data_in_node in input_events_node:
            parsed_data_in = self.parse_data_in_node(data_in_node)
            parsed_nodes.append(parsed_data_in)

        self.coordinator.input_events = parsed_nodes

    def parse_output_events(self, output_events_node):
        parsed_nodes = []
        for data_out_node in output_events_node:
            parsed_data_out = self.parse_data_out_node(data_out_node)
            parsed_nodes.append(parsed_data_out)

        self.coordinator.output_events = parsed_nodes

    def parse_data_in_node(self, data_in_node: ET.Element):
        name = self.__parse_data_event_name(data_in_node)
        dataset = self.__parse_data_event_dataset(data_in_node)
        instance = self.__parse_data_event_instance(data_in_node)
        start_instance = self.__parse_input_event_start_instance(data_in_node)
        end_instance = self.__parse_input_event_end_instance(data_in_node)

        return InputEvent(
            name=name,
            dataset=dataset,
            instance=instance,
            start_instance=start_instance,
            end_instance=end_instance
        )

    def parse_data_out_node(self, data_out_node: ET.Element):
        name = self.__parse_data_event_name(data_out_node)
        dataset = self.__parse_data_event_dataset(data_out_node)
        instance = self.__parse_data_event_instance(data_out_node)
        no_cleanup = self.__parse_data_out_no_cleanup(data_out_node)

        return OutputEvent(
            name=name,
            dataset=dataset,
            instance=instance,
            nocleanup=no_cleanup
        )

    def parse_action_node(self, action_node):
        self.parse_workflow_node(action_node)

    @staticmethod
    def __parse_data_event_name(data_event_node):
        name = data_event_node.attrib["name"]
        return el_parser.translate(name)

    @staticmethod
    def __parse_data_event_dataset(data_event_node):
        dataset = data_event_node.attrib["dataset"]
        return el_parser.translate(dataset)

    @staticmethod
    def __parse_data_event_instance(data_event_node):
        return xml_utils.get_tag_el_text(data_event_node, "instance")

    @staticmethod
    def __parse_input_event_start_instance(data_in_node) -> str:
        return xml_utils.get_tag_el_text(data_in_node, "start-instance")

    @staticmethod
    def __parse_input_event_end_instance(data_in_node) -> str:
        return xml_utils.get_tag_el_text(data_in_node, "end-instance")

    @staticmethod
    def __parse_data_out_no_cleanup(data_out_node) -> bool:
        no_cleanup = xml_utils.get_tag_el_text(data_out_node, "nocleanup")
        return no_cleanup in ["true", "1"]

    @staticmethod
    def parse_workflow_node(action_node) -> Dict:
        workflow_node = xml_utils.find_node_by_tag(action_node, "workflow")

        configuration_properties = None
        app_path = None

        if workflow_node:
            app_path = xml_utils.get_tag_el_text(workflow_node, "app-path")

            configuration_node = xml_utils.find_node_by_tag(workflow_node, "configuration")
            if configuration_node:
                configuration_properties = extract_properties_from_configuration_node(configuration_node)

        return {
            "app_path": app_path,
            "configuration": configuration_properties
        }

    def parse_coordinator_attributes(self):
        self.parse_start_attribute()
        self.parse_end_attribute()
        self.parse_timezone_attribute()
        self.parse_coordinator_frequency()



