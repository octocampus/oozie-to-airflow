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
import unittest

from parameterized import parameterized

import o2a.o2a_libs.functions as functions


class TestElCoordFunctions(unittest.TestCase):


    @parameterized.expand(
        [
            ("3", "*/3 * * * *"),
            ("65", "*/65 * * * *"),
            ("60", "*/60 * * * *")
        ]
    )
    def test_minutes(self, n, expected):
        self.assertEqual(expected, functions.coord.minutes(n))

    def test_hours_with_n_equal_one(self):
        expected = "@hourly"
        self.assertEqual(expected, functions.coord.hours("1"))

    @parameterized.expand(
        [
            ("3", "0 */3 * * *"),
            ("24", "0 */24 * * *"),
            ("25", "0 */25 * * *")
        ]
    )
    def test_hours(self, n, expected):
        self.assertEqual(expected, functions.coord.hours(n))

    def test_days_with_n_equal_one(self):
        expected = "@daily"
        self.assertEqual(expected, functions.coord.days("1"))

    @parameterized.expand(
        [
            ("3", "0 0 */3 * *"),
            ("31", "0 0 */31 * *"),
            ("33","0 0 */33 * *")
        ]
    )
    def test_days(self, n, expected):
        self.assertEqual(expected, functions.coord.days(n))

    @parameterized.expand(
        [
            ("3",),
            ("31",),
            ("33",)
        ]
    )
    def test_end_of_days(self, n):
        expected = f"59 23 */{n} * *"
        self.assertEqual(expected, functions.coord.end_of_days(n))

    def test_months_with_n_equal_one(self):
        expected = "@monthly"
        self.assertEqual(expected, functions.coord.months("1"))

    @parameterized.expand(
        [
            ("3", "0 0 1 */3 *"),
            ("0", "0 0 1 */0 *"),
            ("31", "0 0 1 */31 *"),
            ("33", "0 0 1 */33 *")
        ]
    )
    def test_months(self, n, expected):
        self.assertEqual(expected, functions.coord.months(n))

    @parameterized.expand(
        [
            ("3",),
            ("0",),
            ("31",),
            ("33",)
        ]
    )
    def test_end_of_months(self, n):
        expected = f"59 23 L */{n} *"
        self.assertEqual(expected, functions.coord.end_of_months(n))
