# -*- coding: utf-8 -*-
import os
import shutil

from airflow import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from typing import List, Tuple

from o2a.o2a_libs.utils import construct_hdfs_path_and_alias, handle_archive


class PrepareAction:
    def __init__(self, prepare: List[Tuple[str, str]] = None):
        self.prepare_commands = prepare
        self.subprocess_hook = SubprocessHook()
        self.hadoop_command_option = {"mkdir": "-mkdir", "delete": "-rm -rf"}

    def prepare(self):
        for command, path in self.prepare_commands:
            self.prepare_command(path, command)

    def prepare_command(self, path: str, command: str):
        hdfs_path, _ = construct_hdfs_path_and_alias(path)
        result = self.subprocess_hook.run_command(
            command=[
                shutil.which("bash"),
                "-c",
                f"hadoop fs {self.hadoop_command_option[command]} {hdfs_path}",
            ]
        )
        if result.exit_code != 0:
            raise AirflowException("Error occured while preparing worker, <prepare> tag")


class CopyToWorker:
    def __init__(self, files: List[str] = None, archives: List[str] = None):
        self.files = files
        self.archives = archives
        self.subprocess_hook = SubprocessHook()
        self.bash_path = shutil.which("bash") or "bash"

    def copy_all_to_worker(self):
        if self.files:
            self.copy_files_to_worker()
        if self.archives:
            self.copy_archives_to_worker()

    def copy_files_to_worker(self):
        for file in self.files:
            hdfs_path, alias = construct_hdfs_path_and_alias(file)
            basename = os.path.basename(os.path.normpath(hdfs_path))
            result = self.subprocess_hook.run_command(
                command=[self.bash_path, "-c", f"hadoop fs -get {hdfs_path} . && mv {basename} {alias}"]
            )
            if result.exit_code != 0:
                raise AirflowException("Copy files to worker Failed")

    def copy_archives_to_worker(self):
        for archive in self.archives:
            hdfs_path, alias = construct_hdfs_path_and_alias(archive)
            basename = os.path.basename(os.path.normpath(hdfs_path))
            result = self.subprocess_hook.run_command(
                command=[self.bash_path, "-c", f"hadoop fs -get {hdfs_path} ."]
            )
            if result.exit_code != 0:
                raise AirflowException("Copy archives to worker Failed.")
            handle_archive(basename, alias)
