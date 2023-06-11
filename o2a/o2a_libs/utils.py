# -*- coding: utf-8 -*-
import gzip
import os
import shutil
import subprocess
import tarfile
import zipfile
from airflow.exceptions import AirflowSkipException
from airflow import AirflowException, DAG
from airflow.models import DagRun
from airflow.utils.task_group import TaskGroup


def construct_hdfs_path(context, path):
    if path.startswith("hdfs://"):
        return path
    if path.startswith("/"):
        return context["nameNode"].strip("/") + path
    if "workflowAppUri" in context.keys():
        return os.path.join(context.get("workflowAppUri"), path)
    return os.path.join(context.get("oozie.wf.application.path"), path)


def construct_hdfs_path_and_alias(context, path: str):
    split_path = path.split("#")
    if len(split_path) > 2:
        raise AirflowException("Can't have more than one # in files or archives")
    if len(split_path) <= 2:
        hdfs_path = construct_hdfs_path(context, split_path[0])
        alias = os.path.basename(os.path.normpath(path))
        if len(split_path) == 2:
            alias = split_path[1]

        return hdfs_path, alias


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


def skip_if_upstream_failed(id_dag, id_task):
    state = get_state_of_task(id_dag, id_task)
    if state == "upstream_failed":
        raise AirflowSkipException
def get_state_of_task(id_dag, id_task):
    last_dag_run = DagRun.find(dag_id=id_dag)
    last_dag_run.sort(key=lambda x: x.execution_date, reverse=True)
    state = last_dag_run[0].get_task_instance(id_task).state
    return state

def resolve_subwf_state_state(dag: DAG,taskgroup: TaskGroup):
    upstream_tasks = get_leaves_tasks(taskgroup)
    for task in upstream_tasks:
        state = get_state_of_task(dag.dag_id, task.task_id)
        if state == 'failed':
            raise Exception("subDAG Failed")
        if state == "success":
            return

def get_leaves_tasks(taskgroup: TaskGroup):
    return taskgroup.get_leaves()

def dag_failed_exception() :
    raise Exception("DAG Failed")