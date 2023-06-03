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
"""Tests parsed node"""
import unittest

from o2a.converter.task import Task
from o2a.converter.task_group import TaskGroup, ActionTaskGroup


class TestTaskGroup(unittest.TestCase):
    def setUp(self):
        self.task_group = TaskGroup(
            dag_id="test_dag", name="task1", tasks=[Task(task_id="task1", template_name="dummy.tpl")]
        )
        self.action_task_group = ActionTaskGroup(
            dag_id="test_dag",
            name="action_task",
            tasks=[Task(task_id="action_task", template_name="dummy.tpl")],
        )

    def test_add_downstream_node_name(self):
        self.task_group.downstream_names.append("task1")
        self.assertIn("task1", self.task_group.downstream_names)

    def test_set_downstream_error_node_name(self):
        self.task_group.error_downstream_name = "task1"
        self.assertIn("task1", self.task_group.error_downstream_name)

    def test_add_state_handler_should_add_branch_operator_to_tasks_and_adjust_relations_when_ActionTaskGroup(
        self,
    ):
        self.action_task_group.add_state_handler()
        self.assertEqual(
            self.action_task_group.tasks,
            [
                Task(
                    task_id="action_task",
                    template_name="dummy.tpl",
                    trigger_rule="all_success",
                    template_params={},
                ),
                Task(
                    task_id="action_task_branch",
                    template_name="state_handler.tpl",
                    trigger_rule="all_done",
                    template_params={
                        "task": "action_task_branch",
                        "ok": [],
                        "error": None,
                        "dag_id": "test_dag",
                        "upstream": "action_task",
                    },
                ),
            ],
        )
