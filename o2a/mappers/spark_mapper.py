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
"""Maps Spark action to Airflow Dag"""
from typing import Any, Dict, List, Optional, Type

import xml.etree.ElementTree as ET

from o2a.converter.exceptions import ParseException
from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.property_utils import PropertySet
from o2a.tasks.spark_local_task import SparkLocalTask
from o2a.utils import xml_utils
from o2a.utils.el_utils import replace_jinja_vars_with_known_values_from_props

# pylint: disable=too-many-instance-attributes
from o2a.utils.xml_utils import get_tag_el_text

SPARK_TAG_VALUE = "value"
SPARK_TAG_NAME = "name"
SPARK_TAG_ARG = "arg"
SPARK_TAG_OPTS = "spark-opts"
SPARK_TAG_JOB_NAME = "name"
SPARK_TAG_CLASS = "class"
SPARK_TAG_JAR = "jar"
SPARK_TAG_MASTER = "master"
SPARK_TAG_MODE = "mode"


class SparkMapper(ActionMapper):
    """Maps Spark Action"""

    TASK_MAPPER = {
        "local": SparkLocalTask,
        "ssh": Task,
        "gcp": Task,
    }

    def __init__(self, oozie_node: ET.Element, name: str, props: PropertySet, **kwargs):
        ActionMapper.__init__(self, oozie_node=oozie_node, name=name, props=props, **kwargs)
        self.java_class: Optional[str] = None
        self.master: Optional[str] = None
        self.mode: Optional[str] = None
        self.java_jar: Optional[str] = None
        self.job_name: Optional[str] = None
        self.jars: List[str] = []
        self.application_args: List[str] = []
        self.oozie_files: List[str] = []
        self.oozie_archives: List[str] = []
        self.spark_opts: Dict[str, Any] = {}
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)
        self.tuning_params: Dict[str, str] = {}

    def on_parse_node(self):
        super().on_parse_node()
        self.oozie_files = self._parse_file_archive_nodes("file")
        self.oozie_archives = self._parse_file_archive_nodes("archive")

        self.java_jar = get_tag_el_text(self.oozie_node, tag=SPARK_TAG_JAR)
        self.java_class = get_tag_el_text(self.oozie_node, tag=SPARK_TAG_CLASS)

        self.job_name = get_tag_el_text(self.oozie_node, tag=SPARK_TAG_JOB_NAME)
        self.master = get_tag_el_text(self.oozie_node, tag=SPARK_TAG_MASTER)
        self.mode = get_tag_el_text(self.oozie_node, tag=SPARK_TAG_MODE)
        spark_opts = get_tag_el_text(self.oozie_node, tag=SPARK_TAG_OPTS)
        if spark_opts:
            if "{{" in spark_opts:
                spark_opts = replace_jinja_vars_with_known_values_from_props(
                    spark_opts, self.props.job_properties
                )
            spark_opts_element = ET.Element("spark-opts")
            spark_opts_element.text = spark_opts
            self.spark_opts, self.tuning_params = self._parse_spark_opts(spark_opts_element)

        self.application_args = xml_utils.get_tags_el_array_from_text(self.oozie_node, tag=SPARK_TAG_ARG)

    @staticmethod
    def _parse_spark_opts(spark_opts_node: ET.Element):
        """
        Some examples of the spark-opts element:
        --conf key1=value
        --conf key2="value1 value2"
        """
        conf = {}
        tuning_params = {}
        if spark_opts_node.text:
            spark_opts = spark_opts_node.text.split("--")[1:]
        else:
            raise ParseException(f"Spark opts node has no text: {spark_opts_node}")
        clean_opts = [opt.strip() for opt in spark_opts]
        clean_opts_split = [opt.split(maxsplit=1) for opt in clean_opts]

        for spark_opt in clean_opts_split:
            # Can have multiple "--conf" in spark_opts
            if spark_opt[0] == "conf":
                key, _, value = spark_opt[1].partition("=")
                # Value is required
                if not value:
                    raise ParseException(
                        f"Incorrect parameter format. Expected format: key=value. Current value: {spark_opt}"
                    )
                # Delete surrounding quotes
                if len(value) > 2 and value[0] in ["'", '"'] and value:
                    value = value[1:-1]
                conf[key] = value
            else:
                print(spark_opt)
                tuning_params[spark_opt[0]] = spark_opt[1]

        return conf, tuning_params

    def to_tasks_and_relations(self):

        task_class: Type[Task] = self.get_task_class(self.TASK_MAPPER)
        prepare_commands = self.prepare_extension.parse_prepare_as_list_of_tuples()
        action_task = task_class(
            task_id=self.name,
            template_name="spark/spark.tpl",
            template_params=dict(
                conf={**self.props.action_node_properties, **self.spark_opts},
                name=self.job_name,
                queue=self.tuning_params.get("queue"),
                master=self.master,
                mode=self.mode,
                java_class=self.java_class,
                spark_conn_id=self.props.config["spark_conn_id"]
                if "spark_conn_id" in self.props.config
                else "spark_default_conn_id",
                jars=self.java_jar,
                application_args=self.application_args,
                prepare=prepare_commands,
                oozie_files=self.oozie_files,
                oozie_archives=self.oozie_archives,
                files=self.tuning_params.get("files"),
                archives=self.tuning_params.get("archives"),
                driver_memory=self.tuning_params.get("driver-memory"),
                executor_memory=self.tuning_params.get("executor-memory"),
                executor_cores=self.tuning_params.get("executor-cores"),
                total_executor_cores=self.tuning_params.get("total-executor-cores"),
                num_executors=self.tuning_params.get("num-executors"),
            ),
        )

        tasks = [action_task]
        relations: List[Relation] = []
        return tasks, relations

    def required_imports(self) -> Any:
        # Bash are for the potential prepare statement
        dependencies = self.get_task_class(self.TASK_MAPPER).required_imports()
        prepare_dependencies = self.prepare_extension.required_imports()

        return dependencies.union(prepare_dependencies)
