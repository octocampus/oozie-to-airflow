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
from airflow import AirflowException
from airflow.models import TaskInstance, BaseOperator
from airflow.models.base import Operator
from airflow.utils.session import provide_session
from jinja2 import contextfunction
from typing import Set, Optional, List

from o2a.converter.dataset import Dataset, find_dataset_by_name, get_dataset_name_from_task_doc


def minutes(n: str) -> str:
    return f"*/{n} * * * *"


def hours(n: str) -> str:
    return "@hourly" if n == "1" else f"0 */{n} * * *"


def days(n: str) -> str:
    return "@daily" if n == "1" else f"0 0 */{n} * *"


def months(n: str) -> str:
    return "@monthly" if n == "1" else f"0 0 1 */{n} *"


def end_of_days(n: str) -> str:
    return f"59 23 */{n} * *"


def end_of_months(n: str) -> str:
    return f"59 23 L */{n} *"


@contextfunction
def current(context=None, session=None, n: str=None):
    """
    DS_II : dataset initial-instance (datetime)
    DS_FREQ: dataset frequency (minutes)
    CA_NT: coordinator action creation (materialization) nominal time
    coord:current(int n) = DS_II + DS_FREQ * ( (CA_NT - DS_II) div DS_FREQ + n)
    """
    datasets: Optional[List] = context.get('datasets')
    if datasets is None:
        raise AirflowException("No datasets!")

    task: Optional[BaseOperator] = context.get('task', None)  # pylint:disable=invalid-name

    dataset_name = get_dataset_name_from_task_doc(task.doc)

    dataset = find_dataset_by_name(datasets, dataset_name)

    return f"{dataset['name']}-{n}"





