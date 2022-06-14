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
from typing import List, Optional, Set

# noinspection PyPep8Naming
import xml.etree.ElementTree as ET

from o2a.converter.data_events import InputEvent, OutputEvent
from o2a.converter.dataset import Dataset


# pylint: disable=too-many-instance-attributes
class Coordinator:
    """Class for Coordinator"""

    def __init__(
        self,
        input_directory_path: str,
        dependencies: Set[str] = None,
    ) -> None:

        self.coordinator_file = None
        self.dependencies = dependencies or {"import pendulum", "from o2a.converter.dataset import Dataset"}
        self.input_directory_path = input_directory_path
        self.root_node = None

        self.name: Optional[str] = None
        self.start: Optional[str] = None
        self.end: Optional[str] = None
        self.timezone: Optional[str] = None
        self.frequency: Optional[str] = None

        self.timeout: Optional[str]
        self.concurrency: Optional[str]
        self.execution: Optional[str]

        self.datasets: Optional[List[Dataset]] = None
        self.input_events: Optional[List[InputEvent]] = None
        self.output_events: Optional[List[OutputEvent]] = None

    def get_root_node(self):
        tree = ET.parse(self.coordinator_file)
        return tree.getroot()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False
