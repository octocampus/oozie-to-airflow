# -*- coding: utf-8 -*-
import gzip
import os
import shutil
import zipfile

from airflow.models.baseoperator import BaseOperator

import subprocess
from airflow.exceptions import AirflowException
from typing import List

from jinja2 import contextfunction
import tarfile


@contextfunction
def construct_path(context, path: str) -> str:
    if path.startswith("hdfs://"):
        return path
    if path.startswith("/"):
        return context["nameNode"] + path
    if "workflowAppUri" in context.keys():
        return context.get("workflowAppUri") + "/" + path
    return context.get("oozie.wf.application.path") + "/" + path


def handle_archive(basename: str, alias: str) -> None:
    if basename.endswith("jar"):
        copy_jar = subprocess.Popen(
            f" mv {basename} {alias}", stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True
        )
        output, error = copy_jar.communicate()
        if error:
            raise AirflowException(error)
    elif basename.endswith("zip"):
        zip_obj = zipfile.ZipFile(basename, "r")
        zip_obj.extractall(path=alias)
    elif basename.endswith("tar.gz"):
        with tarfile.open(basename, "r:gz") as tar_object:
            tar_object.extractall(path=alias)
    elif basename.endswith("gz"):
        with gzip.open(basename, "rb") as source_file, open(alias, "wb") as dest_file:
            shutil.copyfileobj(source_file, dest_file)
    elif basename.endswith("tar"):
        with tarfile.open(basename, "r:") as tar_object:
            tar_object.extractall(path=alias)


class FilesOozieOperator(BaseOperator):
    def __init__(self, files: List[str], aliases: List[str], *args, **kwargs) -> None:
        super(FilesOozieOperator, self).__init__(*args, **kwargs)
        self.files = files
        self.aliases = aliases

    def execute(self, context):
        for index, file in enumerate(self.files):
            file = construct_path(context, file)
            basename = os.path.basename(os.path.normpath(file))
            if self.aliases[index] is None:
                self.aliases[index] = basename
            copy_file = subprocess.Popen(
                f"hadoop fs -get {file} . && mv {basename} {self.aliases[index]}",
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                shell=True,
            )
            copy_out, copy_err = copy_file.communicate()
            if copy_err:
                raise AirflowException("Error while copying hdfs files to worker: " + copy_err)


class ArchivesOozieOperator(BaseOperator):
    def __init__(self, archives: List[str], aliases: List[str], *args, **kwargs) -> None:
        super(ArchivesOozieOperator, self).__init__(*args, **kwargs)
        self.archives = archives
        self.aliases = aliases

    def execute(self, context):
        for index, archive in enumerate(self.archives):
            archive = construct_path(context, archive)
            basename = os.path.basename(os.path.normpath(archive))
            copy_file = subprocess.Popen(
                f"hadoop fs -get {archive} .", stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True
            )
            copy_out, copy_err = copy_file.communicate()
            if copy_err:
                raise AirflowException("Error while copying hdfs files to worker: " + copy_err)
            if self.aliases[index] is None:
                self.aliases[index] = basename
            handle_archive(basename, self.aliases[index])
