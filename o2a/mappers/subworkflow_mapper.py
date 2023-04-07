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
"""Maps subworkflow of Oozie to Airflow's sub-dag"""
import logging
import os
from typing import Dict, List, Set, Type

from xml.etree.ElementTree import Element

from o2a.converter.oozie_converter import OozieConverter
from o2a.converter.relation import Relation
from o2a.converter.renderers import BaseRenderer
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.transformers.base_transformer import BaseWorkflowTransformer
from o2a.utils import xml_utils, el_utils

TAG_APP = "app-path"


# pylint: disable=too-many-instance-attributes
class SubworkflowMapper(ActionMapper):
    """
    Converts a Sub-workflow Oozie node to an Airflow task.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        oozie_node: Element,
        name: str,
        dag_name: str,
        input_directory_path: str,
        output_directory_path: str,
        subdag_folder: str,
        props: PropertySet,
        action_mapper: Dict[str, Type[ActionMapper]],
        renderer: BaseRenderer,
        transformers: List[BaseWorkflowTransformer] = None,
        **kwargs,
    ):
        ActionMapper.__init__(
            self,
            oozie_node=oozie_node,
            name=name,
            dag_name=dag_name,
            props=props,
            input_directory_path=input_directory_path,
            **kwargs,
        )
        self.task_id = name
        self.input_directory_path = input_directory_path
        self.output_directory_path = output_directory_path
        self.dag_name = dag_name
        self.subdag_folder = subdag_folder
        self.action_mapper = action_mapper
        self.renderer = renderer
        self.transformers = transformers or []
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        app_path = xml_utils.get_tag_el_text(self.oozie_node, TAG_APP)
        resolved_app_path = el_utils.resolve_job_properties_in_string(app_path, self.props)
        if resolved_app_path[-1:-5] == ".xml":
            app_directory_path, _, _ = resolved_app_path.rpartition("/")
        else:
            app_directory_path = resolved_app_path

        _, _, self.app_name = app_directory_path.rpartition("/")

        parent_path, _, _ = self.input_directory_path.rpartition("/")

        app_path = os.path.join(parent_path, self.app_name)
        if not os.path.exists(self.subdag_folder):
            os.makedirs(self.subdag_folder)
        if not os.path.exists(os.path.join(self.subdag_folder, self.app_name, f"subdag_{self.app_name}.py")):
            os.makedirs(os.path.join(self.subdag_folder, self.app_name), exist_ok=True)
            if self.subdag_folder != self.output_directory_path:
                with open(os.path.join(self.subdag_folder, "__init__.py"), "w"):
                    pass
            logging.info(f"Converting subworkflow from {app_path}")
            converter = OozieConverter(
                input_directory_path=app_path,
                output_directory_path=os.path.join(self.subdag_folder, self.app_name),
                renderer=self.renderer,
                action_mapper=self.action_mapper,
                dag_name=self.app_name,
                initial_props=self.get_child_props(),
                transformers=self.transformers,
                subdag_folder=os.path.join(self.subdag_folder, self.app_name),
            )
            converter.convert(as_subworkflow=True)
        else:
            logging.info(f"subworkflow {app_path} already converted")

    def get_child_props(self) -> PropertySet:
        propagate_configuration = self.oozie_node.find("propagate-configuration")
        # Below the `is not None` is necessary due to Element's __bool__() return value:
        # `len(self._children) != 0`,
        # and `propagate_configuration` is an empty node so __bool__() will always return False.
        return (
            self.props if propagate_configuration is not None else PropertySet(config={}, job_properties={})
        )

    def to_tasks_and_relations(self):
        tasks: List[Task] = [
            Task(task_id=self.name, template_name="subwf.tpl", template_params=dict(app_name=self.app_name))
        ]
        relations: List[Relation] = []
        return tasks, relations

    def required_imports(self) -> Set[str]:
        base_folder = os.path.basename(os.path.normpath(self.subdag_folder))
        return {
            "from airflow.utils import dates",
            "from airflow.contrib.operators import dataproc_operator",
            "from airflow.operators.subdag_operator import SubDagOperator",
            f"import {base_folder}.{self.app_name}.subdag_{self.app_name}",
        }
