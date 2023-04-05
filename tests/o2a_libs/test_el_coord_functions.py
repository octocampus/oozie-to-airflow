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
"""Tests for all functions module"""
import datetime
import unittest

from airflow import AirflowException
from airflow.models import BaseOperator
from parameterized import parameterized

import o2a.o2a_libs.functions as functions
from o2a.converter.dataset import Dataset
from o2a.o2a_libs.el_coord_functions import calculate_current_n, resolve_dataset_template, current


class TestElCoordFunctions(unittest.TestCase):
    @parameterized.expand([("3", "*/3 * * * *"), ("65", "*/65 * * * *"), ("60", "*/60 * * * *")])
    def test_minutes(self, n, expected):
        self.assertEqual(expected, functions.coord.minutes(n))

    def test_hours_with_n_equal_one(self):
        expected = "@hourly"
        self.assertEqual(expected, functions.coord.hours("1"))

    @parameterized.expand([("3", "0 */3 * * *"), ("24", "0 */24 * * *"), ("25", "0 */25 * * *")])
    def test_hours(self, n, expected):
        self.assertEqual(expected, functions.coord.hours(n))

    def test_days_with_n_equal_one(self):
        expected = "@daily"
        self.assertEqual(expected, functions.coord.days("1"))

    @parameterized.expand([("3", "0 0 */3 * *"), ("31", "0 0 */31 * *"), ("33", "0 0 */33 * *")])
    def test_days(self, n, expected):
        self.assertEqual(expected, functions.coord.days(n))

    @parameterized.expand([("3",), ("31",), ("33",)])
    def test_end_of_days(self, n):
        expected = f"59 23 */{n} * *"
        self.assertEqual(expected, functions.coord.end_of_days(n))

    def test_months_with_n_equal_one(self):
        expected = "@monthly"
        self.assertEqual(expected, functions.coord.months("1"))

    @parameterized.expand(
        [("3", "0 0 1 */3 *"), ("0", "0 0 1 */0 *"), ("31", "0 0 1 */31 *"), ("33", "0 0 1 */33 *")]
    )
    def test_months(self, n, expected):
        self.assertEqual(expected, functions.coord.months(n))

    @parameterized.expand([("3",), ("0",), ("31",), ("33",)])
    def test_end_of_months(self, n):
        expected = f"59 23 L */{n} *"
        self.assertEqual(expected, functions.coord.end_of_months(n))

    @parameterized.expand(
        [
            (
                datetime.datetime(2009, 1, 2, 0, 0),
                24 * 60,
                datetime.datetime(2009, 5, 30, 0, 0),
                0,
                datetime.datetime(2009, 5, 30, 0, 0),
            ),
            (
                datetime.datetime(2009, 1, 2, 0, 0),
                24 * 60,
                datetime.datetime(2009, 5, 30, 0, 0),
                1,
                datetime.datetime(2009, 5, 31, 0, 0),
            ),
            (
                datetime.datetime(2009, 1, 8, 0, 0),
                7 * 24 * 60,
                datetime.datetime(2009, 5, 30, 0, 0),
                -3,
                datetime.datetime(2009, 5, 7, 0, 0),
            ),
        ]
    )
    def test_calculate_current_n(self, initial_instance, frequency, execution_time, n, expected):
        self.assertEqual(expected, calculate_current_n(initial_instance, frequency, execution_time, n))

    @parameterized.expand(
        [
            (
                {},
                "hdfs://dataset/{{YEAR}}/{{MONTH}}",
                "2023-05-01T05:00Z",
                "hdfs://dataset/2023/05",
            ),
            ({"user": "test_user"}, "hdfs://test/data", "2020-01-01T00:00Z", "hdfs://test/data"),
            (
                {"user": "test_user"},
                "hdfs://dataset/{{user}}/{{YEAR}}{{MONTH}}/{{DAY}}{{HOUR}}",
                "2023-04-03T12:00Z",
                "hdfs://dataset/test_user/202304/0312",
            ),
        ]
    )
    def test_resolve_dataset_template(self, context, template, date, expected):
        self.assertEqual(expected, resolve_dataset_template(context, template, date))

    def test_current_should_raise_exception_when_no_datasets(self):
        context = {
            "ts": "2023-04-03T11:12:16.590559+00:00",
            "task": BaseOperator(task_id="test_task", doc="dataset_name=test_logs"),
        }
        with self.assertRaises(AirflowException):
            current(context, 0)

    def test_current_should_return_None_when_no_dataset_with_exact_name(self):
        context = {
            "ts": "2023-04-03T11:12:16.590559+00:00",
            "task": BaseOperator(task_id="test_task", doc="dataset_name=test_logs"),
            "datasets": [
                Dataset(
                    name="datain",
                    frequency="10080",
                    initial_instance="2009-01-08T00:00Z",
                    timezone="",
                    uri_template="hdfs://test/{{YEAR}}/{{MONTH}}",
                    done_flag="",
                )
            ],
        }
        self.assertEqual(None, current(context, 0))

    def test_current(self):
        context = {
            "user": "test_user",
            "ts": "2009-05-30T00:00:16.590559+00:00",
            "task": BaseOperator(task_id="test_task", doc="dataset_name=test_logs"),
            "datasets": [
                Dataset(
                    name="test_logs",
                    frequency="10080",
                    initial_instance="2009-01-08T00:00Z",
                    timezone="",
                    uri_template="hdfs://test/{{user}}/{{YEAR}}/{{MONTH}}/{{DAY}}",
                    done_flag="",
                )
            ],
        }
        expected = "hdfs://test/test_user/2009/05/07"
        self.assertEqual(expected, current(context, -3))
