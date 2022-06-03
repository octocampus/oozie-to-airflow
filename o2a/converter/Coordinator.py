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
"""Coordinator"""
from typing import List, Optional, Set, Type

from o2a.converter.data_events import InputEvent, OutputEvent
from o2a.converter.dataset import Dataset
from o2a.converter.oozie_node import OozieNode
from o2a.converter.task_group import TaskGroup

# noinspection PyPep8Naming
import xml.etree.ElementTree as ET


class Coordinator:
    """Class for Coordinator"""

    def __init__(
            self,
            input_directory_path: str,
            dependencies: Set[str] = None,

    ) -> None:

        self.coordinator_file = None
        self.dependencies = dependencies or {

        }
        self.input_directory_path = input_directory_path
        self.root_node = None

        self.name: str
        self.start: str
        self.end: str
        self.timezone: str
        self.frequency: str = None

        self.timeout: Optional[int]
        self.concurrency: Optional[int]
        self.execution: Optional[str]

        self.datasets: Optional[List[Dataset]] = None
        self.input_events: Optional[List[InputEvent]] = None
        self.output_events: Optional[List[OutputEvent]] = None


    def get_root_node(self):
        tree = ET.parse(self.coordinator_file)
        return tree.getroot()

    def get_nodes_by_type(self, mapper_type: Type):
        return [node for node in self.nodes.values() if isinstance(node.mapper, mapper_type)]

    def find_upstream_nodes(self, target_node):
        result = []
        for node in self.nodes.values():
            if target_node.name in node.downstream_names or target_node.name == node.error_downstream_name:
                result.append(node)
        return result

    def find_upstream_task_group(self, target_task_group) -> List[TaskGroup]:
        result = []
        for task_group in self.task_groups.values():
            if (
                    target_task_group.name in task_group.downstream_names
                    or target_task_group.name == task_group.error_downstream_name
            ):
                result.append(task_group)
        return result

    def get_task_group_without_upstream(self) -> List[TaskGroup]:
        task_groups = []
        for task_group in self.task_groups.values():
            upstream_task_group = self.find_upstream_task_group(task_group)
            if not upstream_task_group:
                task_groups.append(task_group)
        return task_groups

    def get_task_group_without_ok_downstream(self):
        task_groups = []
        for task_group in self.task_groups.values():
            if not task_group.downstream_names:
                task_groups.append(task_group)
        return task_groups

    def get_task_group_without_error_downstream(self):
        task_groups = []
        for task_group in self.task_groups.values():
            if not task_group.error_downstream_name:
                task_groups.append(task_group)
        return task_groups

    def remove_node(self, node_to_delete: OozieNode):
        del self.nodes[node_to_delete.name]

        for node in self.nodes.values():
            if node_to_delete.name in node.downstream_names:
                node.downstream_names.remove(node_to_delete.name)
            if node.error_downstream_name == node_to_delete.name:
                node.error_downstream_name = None

    def __repr__(self) -> str:
        return (
            f'Workflow(dag_name="{self.dag_name}", input_directory_path="{self.input_directory_path}", '
            f'output_directory_path="{self.output_directory_path}", relations={self.task_group_relations}, '
            f"nodes={self.nodes.keys()}, dependencies={self.dependencies})"
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False

