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
"""All Coord EL functions"""
import re
from datetime import datetime, timedelta

from airflow import AirflowException
from airflow.models import BaseOperator
from jinja2 import contextfunction
from typing import List, Optional

from o2a.converter.dataset import Dataset, find_dataset_by_name, get_dataset_name_from_task_doc


def minutes(n: str) -> str:  # pylint:disable=invalid-name
    return f"*/{n} * * * *"


def hours(n: str) -> str:  # pylint:disable=invalid-name
    return "@hourly" if n == "1" else f"0 */{n} * * *"


def days(n: str) -> str:  # pylint:disable=invalid-name
    return "@daily" if n == "1" else f"0 0 */{n} * *"


def months(n: str) -> str:  # pylint:disable=invalid-name
    return "@monthly" if n == "1" else f"0 0 1 */{n} *"


def end_of_days(n: str) -> str:  # pylint:disable=invalid-name
    return f"59 23 */{n} * *"


def end_of_months(n: str) -> str:  # pylint:disable=invalid-name
    return f"59 23 L */{n} *"


@contextfunction
def current(context=None, n: int = None):  # pylint:disable=invalid-name
    """
    DS_II : dataset initial-instance (datetime)
    DS_FREQ: dataset frequency (minutes)
    CA_NT: coordinator action creation (materialization) nominal time
    coord:current(int n) = DS_II + DS_FREQ * ( (CA_NT - DS_II) div DS_FREQ + n)
    """
    datasets: Optional[List[Dataset]] = context.get("datasets")
    if datasets is None:
        raise AirflowException("No datasets!")

    task: Optional[BaseOperator] = context.get("task", None)  # pylint:disable
    if task:
        dataset_name = get_dataset_name_from_task_doc(task.doc)

        dataset = find_dataset_by_name(datasets, dataset_name)

        if dataset:
            exec_time = context.get("ts")[:16]
            dag_run = datetime.strptime(exec_time, "%Y-%m-%dT%H:%M")
            initial_instance = datetime.strptime(dataset.initial_instance[:-1], "%Y-%m-%dT%H:%M")
            template = dataset.uri_template
            template = template.replace("{", "")
            template = template.replace("}", "")
            frequency = int(dataset.frequency)
            current_n = initial_instance + timedelta(
                minutes=frequency * (((dag_run - initial_instance).total_seconds() // 60) // frequency + n)
            )
            resolve_template_map = {
                r"\$MINUTE": str(current_n.minute)
                if len(str(current_n.minute)) > 1
                else "0" + str(current_n.minute),
                r"\$HOUR": str(current_n.hour) if len(str(current_n.hour)) > 1 else "0" + str(current_n.hour),
                r"\$DAY": str(current_n.day) if len(str(current_n.day)) > 1 else "0" + str(current_n.day),
                r"\$MONTH": str(current_n.month)
                if len(str(current_n.month)) > 1
                else "0" + str(current_n.month),
                r"\$YEAR": str(current_n.year),
            }
            for word, replacement in resolve_template_map.items():
                template = re.sub(word, replacement, template)
            return template

    return None
