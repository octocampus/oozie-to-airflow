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

from typing import Any, Dict
from airflow.utils.trigger_rule import TriggerRule
from o2a.converter.task import Task


class SparkLocalTask(Task):
    """Class for Spark Local execution Task"""

    def __init__(
            self,
            task_id: str,
            template_name: str,
            trigger_rule: str =
            TriggerRule.ONE_SUCCESS,
            template_params: Dict[str, Any] = None
    ):

        super().__init__(task_id, template_name, trigger_rule, template_params)

    @staticmethod
    def required_imports() -> set[str]:
        return {
            "from airflow.providers.apache.spark.operators import spark_submit"
        }
