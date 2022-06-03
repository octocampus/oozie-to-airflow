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
import datetime


def minutes(n: int):
    return datetime.timedelta(minutes=n)


def hours(n: int):
    if n == 1: return "@hourly"
    return datetime.timedelta(hours={n})


def days(n: int):
    if n == 1: return "@daily"
    return datetime.timedelta(days={n})


def months(n: int):
    if n == 1: return "@monthly"
    return f"0 * * */{n} *"


def end_of_days(n: int):
    return f"59 23 */{n} * *"


def end_of_months(n: int):
    return f"59 23 L */{n} *"
