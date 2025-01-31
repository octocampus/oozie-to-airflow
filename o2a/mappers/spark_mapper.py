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
from typing import Dict, List, Optional, Set, Type

import xml.etree.ElementTree as ET


from o2a.converter.exceptions import ParseException
from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.property_utils import PropertySet
from o2a.tasks.spark_local_task import SparkLocalTask
from o2a.utils import xml_utils
from o2a.utils.file_archive_extractors import FileExtractor, ArchiveExtractor


# pylint: disable=too-many-instance-attributes
from o2a.utils.xml_utils import get_tag_el_text

SPARK_TAG_VALUE = "value"
SPARK_TAG_NAME = "name"
SPARK_TAG_ARG = "arg"
SPARK_TAG_OPTS = "spark-opts"
SPARK_TAG_JOB_NAME = "name"
SPARK_TAG_CLASS = "class"
SPARK_TAG_JAR = "jar"


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
        self.java_jar: Optional[str] = None
        self.job_name: Optional[str] = None
        self.jars: List[str] = []
        self.application_args: List[str] = []
        self.file_extractor = FileExtractor(oozie_node=oozie_node, props=self.props)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, props=self.props)
        self.hdfs_files: List[str] = []
        self.hdfs_archives: List[str] = []
        self.spark_opts: Dict[str, str] = {}
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)

    def on_parse_node(self):
        super().on_parse_node()
        _, self.hdfs_files = self.file_extractor.parse_node()
        _, self.hdfs_archives = self.archive_extractor.parse_node()

        self.java_jar = get_tag_el_text(self.oozie_node, tag=SPARK_TAG_JAR)
        self.java_class = get_tag_el_text(self.oozie_node, tag=SPARK_TAG_CLASS)

        self.job_name = get_tag_el_text(self.oozie_node, tag=SPARK_TAG_JOB_NAME)

        spark_opts = xml_utils.find_nodes_by_tag(self.oozie_node, SPARK_TAG_OPTS)
        if spark_opts:
            self.spark_opts.update(self._parse_spark_opts(spark_opts[0]))

        self.application_args = xml_utils.get_tags_el_array_from_text(self.oozie_node, tag=SPARK_TAG_ARG)

    @staticmethod
    def _parse_spark_opts(spark_opts_node: ET.Element):
        """
        Some examples of the spark-opts element:
        --conf key1=value
        --conf key2="value1 value2"
        """
        conf: Dict[str, str] = {}
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
            # TODO: parse also other options (like --executor-memory 20G --num-executors 50 and many more)
            #  see: https://oozie.apache.org/docs/5.1.0/DG_SparkActionExtension.html#PySpark_with_Spark_Action

        return conf

    def to_tasks_and_relations(self):

        task_class: Type[Task] = self.get_task_class(self.TASK_MAPPER)
        action_task = task_class(
            task_id=self.name,
            template_name="spark/spark.tpl",
            template_params=dict(
                application=self.java_jar,
                conf=self.spark_opts,
                spark_conn_id=self.props.config["spark_cli_conn_id"],
                jars=self.jars,
                java_class=self.java_class,
                name=self.job_name,
                application_args=self.application_args,
            ),
        )

        tasks = [action_task]
        relations: List[Relation] = []
        prepare_task = self.prepare_extension.get_prepare_task()
        if prepare_task:
            tasks, relations = self.prepend_task(prepare_task, tasks, relations)
        return tasks, relations

    def required_imports(self) -> Set[str]:
        # Bash are for the potential prepare statement
        dependencies = self.get_task_class(self.TASK_MAPPER).required_imports()
        prepare_dependencies = self.prepare_extension.required_imports()

        return dependencies.union(prepare_dependencies)
