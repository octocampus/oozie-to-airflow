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
Add Data events Transformer
"""
import re
from typing import Optional, List

from o2a.converter.data_events import InputEvent
from o2a.converter.dataset import find_dataset_by_name, Dataset
from o2a.converter.task import Task
from o2a.converter.task_group import TaskGroup
from o2a.converter.workflow import Workflow
from o2a.o2a_libs.property_utils import PropertySet
from o2a.transformers.base_transformer import BaseWorkflowTransformer

INPUT_EVENTS_TASK_GROUP_NAME = "input_events"


# pylint: disable=too-few-public-methods
class AddWorkflowDataEventsSensorsTransformer(BaseWorkflowTransformer):
    """
    Add workflow data events task group
    """

    def process_workflow_after_convert_nodes(self, workflow: Workflow, props: PropertySet):
        if workflow.coordinator:
            input_events: Optional[List[InputEvent]] = workflow.coordinator.input_events

            if not input_events:
                return

            self._add_input_events_sensors_task_group(input_events, workflow, props)

    @classmethod
    def _add_input_events_sensors_task_group(cls, input_events, workflow, props: Optional[PropertySet]):
        input_events_task_group = cls._create_input_events_task_group(
            workflow=workflow, input_events=input_events, props=props
        )

        input_events_task_group.downstream_names = [
            task_group.name for task_group in workflow.get_task_group_without_upstream()
        ]
        workflow.task_groups[input_events_task_group.name] = input_events_task_group

    @staticmethod
    def _create_input_events_task_group(
        workflow: Workflow, input_events: List[InputEvent], props
    ) -> TaskGroup:
        """
        Creates airflow sensors in a taskgroup to replace input event in oozie
        """

        tasks: List[Task] = []

        for input_event in input_events:
            datasets: Optional[List[Dataset]] = None
            if workflow.coordinator and workflow.coordinator.datasets:
                datasets = workflow.coordinator.datasets

            dataset: Optional[Dataset] = find_dataset_by_name(datasets=datasets, name=input_event.dataset)
            task = Task(
                task_id=input_event.name,
                template_name="input_events_sensor.tpl",
                template_params=dict(
                    uri_template=dataset.uri_template,
                    instance=input_event.instance.strip("{}") if input_event.instance else None,
                    start_instance_n=re.findall(r"-?\d+", input_event.start_instance)[0]
                    if input_event.start_instance
                    else None,
                    end_instance_n=re.findall(r"-?\d+", input_event.end_instance)[0]
                    if input_event.end_instance
                    else None,
                    doc=f"dataset_name={dataset.name}",  # type: ignore
                    poke_interval="60",
                    timeout=props.config["data_events_sensors_timeout"]
                    if "data_events_sensors_mode" in props.config
                    else "60 * 60 * 2",  # TODO change it according to the workflow schedule_interval
                    mode=props.config["data_events_sensors_mode"]
                    if "data_events_sensors_mode" in props.config
                    else "reschedule",
                    hdfs_conn_id=props.config["hdfs_conn_id"]
                    if "hdfs_conn_id" in props.config
                    else "hdfs_default_conn_id",
                ),
            )
            tasks.append(task)

        input_events_task_group = TaskGroup(
            name=INPUT_EVENTS_TASK_GROUP_NAME,
            tasks=tasks,
            dependencies={"from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor"},
        )
        return input_events_task_group
