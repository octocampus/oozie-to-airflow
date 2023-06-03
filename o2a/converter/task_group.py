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
"""Class for Airflow's task group"""
# noinspection PyPackageRequirements
from typing import List, Optional

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.converter.relation import Relation


class TaskGroup:
    """Airflow's tasks group

    It is created as a result of converting the ParsedActionNode object. It contains all of its
    information except the mapper.

    Additionally, it contains an Airflow tasks, relations and python imports statements.
    """

    def __init__(
        self,
        dag_id,
        name,
        tasks,
        relations=None,
        downstream_names=None,
        error_downstream_name=None,
        dependencies=None,
    ):
        self.dag_id = dag_id
        self.name = name
        self.tasks: List[Task] = tasks or []
        self.relations: List[Relation] = relations or []
        self.downstream_names: List[str] = downstream_names or []
        self.error_downstream_name: Optional[str] = error_downstream_name
        self.dependencies: List[str] = dependencies or []
        self.handler_task: Optional[Task] = None

    @property
    def first_task_id(self) -> str:
        """
        Returns task_id of first task in group
        """
        return self.tasks[0].task_id

    def add_state_handler(self):
        """
        Add additional task and relations to handle error and ok flow.
        """
        self.handler_task = Task(
            task_id=self.name + "_branch",
            template_name="state_handler.tpl",
            trigger_rule=TriggerRule.ALL_DONE,
            template_params=dict(
                task=self.name + "_branch",
                ok=self.downstream_names,
                error=self.error_downstream_name,
                dag_id=self.dag_id,
                upstream=self.name,
            ),
        )

        self.relations.append(
            Relation(from_task_id=self.tasks[-1].task_id, to_task_id=self.handler_task.task_id)
        )
        self.tasks.append(self.handler_task)

    @property
    def all_tasks(self):
        return [*self.tasks]

    def __repr__(self) -> str:
        return (
            f"TaskGroup(name={self.name}, "
            f"downstream_names={self.downstream_names}, "
            f"error_downstream_name={self.error_downstream_name}, "
            f"tasks={self.tasks}, relations={self.relations})"
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False


class ActionTaskGroup(TaskGroup):
    pass


class ControlTaskGroup(TaskGroup):
    pass


class NotificationTaskGroup(TaskGroup):
    pass


class StatusNotificationTaskGroup(NotificationTaskGroup):
    pass


class TransitionNotificationTaskGroup(NotificationTaskGroup):
    pass
