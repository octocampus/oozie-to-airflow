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


def calculate_current_n(
    initial_instance: datetime, frequency: int, execution_time: datetime, n: int
) -> datetime:
    """
    Calculate the datetime of the dataset produced shifted by n
    calculated using the following formula :
    initial_instance : dataset initial-instance (datetime)
    frequency : dataset frequency (minutes)
    execution_time: coordinator action creation (materialization) nominal time <=> Dag execution time
    return initial_instance + frequency * ((execution_time - initial_instance) div frequency + n)
    """
    current_n = initial_instance + timedelta(
        minutes=frequency * (((execution_time - initial_instance).total_seconds() // 60) // frequency + n)
    )
    return current_n


@contextfunction
def resolve_dataset_template(context, template: str, datetime_data: str) -> str:
    """
    Given a template, resolve the variables MINUTE, HOUR, MONTH, DAY, YEAR using the datetime data provided
    :param template: string with variables MINUTE... inside ${...} e.g : hdfs://test/${YEAR}/${MONTH}
    :param datetime_data: datetime information e.g : 2023-05-01T05:00
    :return: string representing resolved template e.g : hdfs://test/2023/05
    """

    datetime_data = datetime.strptime(datetime_data[:-1], "%Y-%m-%dT%H:%M")
    resolve_template_map = {
        r"{{MINUTE}}": str(datetime_data.minute)
        if len(str(datetime_data.minute)) > 1
        else "0" + str(datetime_data.minute),
        r"{{HOUR}}": str(datetime_data.hour)
        if len(str(datetime_data.hour)) > 1
        else "0" + str(datetime_data.hour),
        r"{{DAY}}": str(datetime_data.day)
        if len(str(datetime_data.day)) > 1
        else "0" + str(datetime_data.day),
        r"{{MONTH}}": str(datetime_data.month)
        if len(str(datetime_data.month)) > 1
        else "0" + str(datetime_data.month),
        r"{{YEAR}}": str(datetime_data.year),
    }
    for word, replacement in resolve_template_map.items():
        template = re.sub(word, replacement, template)
    el = re.findall(r"\{{.*?\}}", template)

    for w in el:
        template = template.replace(w, context.get(w.strip("{}")))
    return template


@contextfunction
def current(context, n: int):
    datasets: Optional[List[Dataset]] = context.get("datasets")
    if not datasets:
        raise AirflowException("No datasets")
    task: Optional[BaseOperator] = context.get("task", None)  # pylint:disable

    if task:
        dataset_name = get_dataset_name_from_task_doc(task.doc)

        dataset = find_dataset_by_name(datasets, dataset_name)

        if dataset:
            execution_time = context.get("ts")[:16]
            execution_time = datetime.strptime(execution_time, "%Y-%m-%dT%H:%M")
            initial_instance = datetime.strptime(dataset.initial_instance[:-1], "%Y-%m-%dT%H:%M")
            frequency = int(dataset.frequency)
            template = dataset.uri_template
            current_n = calculate_current_n(initial_instance, frequency, execution_time, n)
            dataset_uri = resolve_dataset_template(context, template, current_n.strftime("%Y-%m-%dT%H:%MZ"))
            return dataset_uri

    return None
