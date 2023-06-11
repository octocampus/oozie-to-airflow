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
"""
Converts Oozie application workflow into Airflow's DAG
"""
import shutil
from typing import Dict, Type, List

import os

import logging

from o2a.converter import workflow_xml_parser, coordinator_xml_parser
from o2a.converter.oozie_node import OozieNode, OozieControlNode, OozieActionNode
from o2a.converter.property_parser import PropertyParser
from o2a.converter.relation import Relation
from o2a.converter.renderers import BaseRenderer
from o2a.converter.task_group import TaskGroup, ControlTaskGroup, ActionTaskGroup
from o2a.converter.workflow import Workflow
from o2a.converter.coordinator import Coordinator
from o2a.utils import el_utils
from o2a.utils.file_utils import get_lib_files
from o2a.mappers.action_mapper import ActionMapper
from o2a.transformers.base_transformer import BaseWorkflowTransformer
from o2a.o2a_libs.property_utils import PropertySet


# pylint: disable=too-many-instance-attributes
class OozieConverter:
    """
    Converts Oozie Workflow app to Airflow's DAG

    Each WorkflowXmlParser class corresponds to one workflow, where one can get
    the workflow's required dependencies (imports), operator relations,
    and operator execution sequence.

    :param dag_name: Desired output DAG name.
    :param input_directory_path: Oozie workflow directory.
    :param output_directory_path: Desired output directory.
    :param action_mapper: List of charters that support action nodes
    :param renderer: Renderer that will be used for the output file
    :param transformers: List of transformers that will transform a workflow
    :param user: Username.  # TODO remove me and use real ${user} EL
    :param initial_props: Initial PropertySet object
    """

    def __init__(
            self,
            dag_name: str,
            input_directory_path: str,
            output_directory_path: str,
            action_mapper: Dict[str, Type[ActionMapper]],
            renderer: BaseRenderer,
            transformers: List[BaseWorkflowTransformer] = None,
            user: str = None,
            initial_props: PropertySet = None,
            subdag_folder: str = None,
            as_subworkflow: bool = False,
    ):

        self.coordinator = Coordinator(
            input_directory_path=input_directory_path,
        )

        self.workflow = Workflow(
            dag_name=dag_name,
            input_directory_path=input_directory_path,
            output_directory_path=output_directory_path,
            coordinator=self.coordinator,
            subdag_folder=subdag_folder,
        )
        self.renderer = renderer

        self.transformers = transformers or []
        # Propagate the configuration in case initial property set is passed
        job_properties = {} if not initial_props else initial_props.job_properties
        job_properties["user.name"] = user or os.environ["USER"]
        self.props = PropertySet(job_properties=job_properties)
        self.property_parser = PropertyParser(props=self.props, workflow=self.workflow)

        self.coordinator_parser = coordinator_xml_parser.CoordinatorXmlParser(
            props=self.props,
            action_mapper=action_mapper,
            renderer=renderer,
            coordinator=self.coordinator,
            transformers=transformers,
        )
        self.parser = workflow_xml_parser.WorkflowXmlParser(
            props=self.props,
            action_mapper=action_mapper,
            renderer=self.renderer,
            workflow=self.workflow,
            transformers=self.transformers,
        )
        self.as_subworkflow = as_subworkflow

    def retrieve_lib_jar_libraries(self):
        logging.info(f"Looking for jar libraries for the workflow in {self.workflow.library_folder}.")
        self.workflow.jar_files = get_lib_files(self.workflow.library_folder, extension=".jar")

    def recreate_output_directory(self):
        shutil.rmtree(self.workflow.output_directory_path, ignore_errors=True)
        os.makedirs(self.workflow.output_directory_path, exist_ok=True)

    def convert(self, as_subworkflow=False):
        """
        Starts the process of converting the workflow.
        """
        self.coordinator_parser.parse_coordinator()
        self.retrieve_lib_jar_libraries()
        self.property_parser.parse_property()

        self.parser.parse_workflow()
        self.schedule_workflow()

        self.apply_preconvert_transformers()
        self.convert_nodes()
        self.apply_postconvert_transformers()
        self.add_state_handlers()
        self.convert_relations()
        self.convert_dependencies()

        if as_subworkflow:
            self.renderer.create_subworkflow_file(workflow=self.workflow, props=self.props)
        else:
            self.renderer.create_workflow_file(workflow=self.workflow, props=self.props)

        self.copy_extra_assets()

    def convert_nodes(self):
        """
        For each Oozie node, converts it into relations and internal relations.

        It uses the mapper, which is stored in ParsedActionNode. The result is saved in ParsedActionNode.tasks
        and ParsedActionNode.relations
        """
        from o2a.mappers.decision_mapper import DecisionMapper
        from o2a.mappers.subworkflow_mapper import SubworkflowMapper
        logging.info("Converting nodes to tasks and inner relations")
        for name, oozie_node in self.workflow.nodes.copy().items():
            tasks, relations = oozie_node.mapper.to_tasks_and_relations()
            oozie_node.tasks = tasks

            dependencies = oozie_node.mapper.required_imports()

            oozie_node.relations = relations
            self.workflow.task_groups[name] = self._get_task_group_type(oozie_node)(
                name=name,
                tasks=tasks,
                relations=relations,
                dependencies=dependencies,
                downstream_names=oozie_node.downstream_names,
                error_downstream_name=oozie_node.error_downstream_name,
                decision=isinstance(oozie_node.mapper, DecisionMapper),
                subworkflow=isinstance(oozie_node.mapper, SubworkflowMapper)
            )

    @staticmethod
    def _get_task_group_type(oozie_node: OozieNode) -> Type[TaskGroup]:
        if isinstance(oozie_node, OozieControlNode):
            return ControlTaskGroup
        if isinstance(oozie_node, OozieActionNode):
            return ActionTaskGroup
        return TaskGroup

    def convert_dependencies(self) -> None:
        logging.info("Converting dependencies.")
        for task_group in self.workflow.task_groups.values():
            self.workflow.dependencies.update(task_group.dependencies)

        self.workflow.dependencies.update(self.coordinator.dependencies)

    def convert_relations(self) -> None:
        logging.info("Converting relations between tasks groups.")
        for task_group in self.workflow.task_groups.values():
            if task_group.decision:
                for index in range(len(task_group.downstream_names)):
                    relation = Relation(
                        from_task_id=task_group.downstream_names[index] + "_upstream",
                        to_task_id=task_group.downstream_names[index],
                    )
                    self.workflow.task_group_relations.add(relation)
            else:
                for downstream in task_group.downstream_names:
                    relation = Relation(
                        from_task_id=task_group.last_task_id_of_ok_flow,
                        to_task_id=self.workflow.task_groups[downstream].first_task_id,
                    )
                    self.workflow.task_group_relations.add(relation)
                error_downstream = task_group.error_downstream_name
                if error_downstream:
                    relation = Relation(
                        from_task_id=task_group.last_task_id_of_error_flow,
                        to_task_id=self.workflow.task_groups[error_downstream].first_task_id,
                        is_error=True,
                    )
                    self.workflow.task_group_relations.add(relation)

    def add_state_handlers(self) -> None:
        logging.info("Adding error handlers")
        for node in self.workflow.task_groups.values():
            node.add_state_handler_if_needed(self.workflow.dag_name, self.as_subworkflow)

    def copy_extra_assets(self):
        """
        Copies additional assets needed to execute a workflow, eg. Pig scripts.
        """

        input_script_path = el_utils.resolve_job_properties_in_string(
            os.path.join(self.workflow.input_directory_path, "scripts"), self.props
        )

        if os.path.exists(input_script_path):
            shutil.copytree(
                input_script_path,
                el_utils.resolve_job_properties_in_string(
                    os.path.join(self.workflow.output_directory_path, "scripts"), self.props
                ),
                dirs_exist_ok=True,
            )
        logging.info("Extra assets copied.")

    def apply_preconvert_transformers(self):
        logging.info("Applying pre-convert transformers")
        for transformer in self.transformers:
            transformer.process_workflow_after_parse_workflow_xml(self.workflow)

    def apply_postconvert_transformers(self):
        logging.info("Applying post-convert transformers")
        for transformer in self.transformers:
            transformer.process_workflow_after_convert_nodes(self.workflow, props=self.props)

    def schedule_workflow(self):
        logging.info(f"Applying scheduling params to {self.workflow.dag_name} ")

        self.workflow.schedule_interval = self.coordinator.frequency
        self.workflow.start_date = self.coordinator.start
        self.workflow.end_date = self.coordinator.end
