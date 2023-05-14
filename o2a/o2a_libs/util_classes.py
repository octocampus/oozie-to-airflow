# -*- coding: utf-8 -*-
import os
import shutil

from airflow import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from typing import List, Optional

from o2a.o2a_libs.utils import construct_hdfs_path_and_alias, handle_archive


class PrepareAction:
    def __int__(self, mkdir: Optional[List[str]] = None, delete: Optional[List[str]] = None):
        self.mkdir = mkdir
        self.delete = delete
        self.subprocess_hook = SubprocessHook()
        self.bash_path = shutil.which("bash") or "bash"

    def prepare(self):
        if self.mkdir:
            self._prepare_mkdir()
        if self.delete:
            self._prepare_delete()

    def _prepare_mkdir(self):
        for path in self.mkdir:
            hdfs_path, _ = construct_hdfs_path_and_alias(path)
            result = self.subprocess_hook.run_command(
                command=[self.bash_path, "-c", f"hadoop fs -mkdir {hdfs_path}"]
            )
            if result.exit_code != 0:
                raise AirflowException("Error occured while creating hdfs directory <prepare mkdir tag>.")

    def _prepare_delete(self):
        for path in self.delete:
            hdfs_path, _ = construct_hdfs_path_and_alias(path)
            result = self.subprocess_hook.run_command(
                command=[self.bash_path, "-c", f"hadoop fs -rm -rf {hdfs_path}"]
            )
            if result.exit_code != 0:
                raise AirflowException("Error occured while deleting hdfs directory <prepare delete tag>.")


class CopyToWorker:
    def __int__(self, files: List[str] = None, archives: List[str] = None):
        self.files = files
        self.archives = archives
        self.subprocess_hook = SubprocessHook()
        self.bash_path = shutil.which("bash") or "bash"

    def copy_all_to_worker(self):
        if self.files:
            self._copy_files_to_worker()
        if self.archives:
            self._copy_archives_to_worker()

    def _copy_files_to_worker(self):
        for file in self.files:
            hdfs_path, alias = construct_hdfs_path_and_alias(file)
            basename = os.path.basename(os.path.normpath(hdfs_path))
            result = self.subprocess_hook.run_command(
                command=[self.bash_path, "-c", f"hadoop fs -get {hdfs_path} . && mv {basename} {alias}"]
            )
            if result.exit_code != 0:
                raise AirflowException("Copy files to worker Failed.")

    def _copy_archives_to_worker(self):
        for archive in self.archives:
            hdfs_path, alias = construct_hdfs_path_and_alias(archive)
            basename = os.path.basename(os.path.normpath(hdfs_path))
            result = self.subprocess_hook.run_command(
                command=[self.bash_path, "-c", f"hadoop fs -get {archive} ."]
            )
            if result.exit_code != 0:
                raise AirflowException("Copy archives to worker Failed.")
            handle_archive(basename, alias)
