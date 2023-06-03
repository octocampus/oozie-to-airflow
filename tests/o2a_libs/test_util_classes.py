# -*- coding: utf-8 -*-
import shutil
import unittest

import mock
from airflow import AirflowException

from o2a.o2a_libs.util_classes import PrepareAction, CopyToWorker


class TestPrepareAction(unittest.TestCase):
    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_prepare_command_with_command_mkdir(self, mock_func, mock_subpr):
        context = {"nameNode": "hdfs://namenode:5000/"}
        mock_subpr.return_value.run_command.return_value.exit_code = 0
        mock_func.return_value = "hdfs://test", "alias"
        prepare_action = PrepareAction()
        prepare_action.prepare_command(context, "test", "mkdir")
        mock_subpr.return_value.run_command.assert_called_with(
            command=[shutil.which("bash"), "-c", "hadoop fs -mkdir hdfs://test"]
        )

    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_prepare_command_with_command_delete(self, mock_func, mock_subpr):
        context = {"nameNode": "hdfs://namenode:5000/"}
        mock_subpr.return_value.run_command.return_value.exit_code = 0
        mock_func.return_value = "hdfs://test", "alias"
        prepare_action = PrepareAction()
        prepare_action.prepare_command(context, "test", "delete")
        mock_subpr.return_value.run_command.assert_called_with(
            command=[shutil.which("bash"), "-c", "hadoop fs -rm -rf hdfs://test"]
        )

    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_prepare_command_should_raise_exception_when_command_fails(self, mock_func, mock_subpr):
        context = {"nameNode": "hdfs://namenode:5000/"}
        mock_subpr.return_value.run_command.return_value.exit_code = 1
        mock_func.return_value = "hdfs://test", "alias"
        prepare_action = PrepareAction()
        with self.assertRaisesRegex(AirflowException, "Error occured while preparing worker, <prepare> tag"):
            prepare_action.prepare_command(context, "test", "delete")

    @mock.patch.object(PrepareAction, "prepare_command", autospec=True)
    def test_prepare(self, mock_prepare):
        context = {"nameNode": "hdfs://namenode:5000/"}

        prepare_action = PrepareAction(prepare=[("mkdir", "test")])
        prepare_action.prepare(context)
        mock_prepare.assert_called_with(prepare_action, context, "test", "mkdir")


class TestCopyToWorker(unittest.TestCase):
    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_copy_files_to_worker_should_copy_files(self, mock_func, mock_subpr):
        context = {"nameNode": "hdfs://namenode:5000/"}

        mock_subpr.return_value.run_command.return_value.exit_code = 0
        mock_func.return_value = "hdfs://test", "alias"
        copy_to_worker = CopyToWorker(files=["test"])
        copy_to_worker.copy_files_to_worker(context)
        mock_func.assert_called_with(context, "test")
        mock_subpr.return_value.run_command.assert_called_with(
            command=[shutil.which("bash"), "-c", "hadoop fs -get hdfs://test . && mv test alias"]
        )

    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_copy_files_should_raise_exception_when_copying_failed(self, mock_func, mock_subpr):
        context = {"nameNode": "hdfs://namenode:5000/"}

        mock_subpr.return_value.run_command.return_value.exit_code = 1
        mock_func.return_value = "hdfs://test", "alias"
        copy_to_worker = CopyToWorker(files=["test"])
        with self.assertRaisesRegex(AirflowException, "Copy files to worker Failed"):
            copy_to_worker.copy_files_to_worker(context)
        mock_func.assert_called_with(context, "test")

    @mock.patch("o2a.o2a_libs.util_classes.handle_archive")
    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_copy_archives_to_worker_should_copy_archives(self, mock_func, mock_subpr, mock_archive):
        context = {"nameNode": "hdfs://namenode:5000/"}

        mock_subpr.return_value.run_command.return_value.exit_code = 0
        mock_func.return_value = "hdfs://test", "alias"
        copy_to_worker = CopyToWorker(archives=["test"])
        copy_to_worker.copy_archives_to_worker(context)
        mock_func.assert_called_with(context, "test")
        mock_subpr.return_value.run_command.assert_called_with(
            command=[shutil.which("bash"), "-c", "hadoop fs -get hdfs://test ."]
        )
        mock_archive.assert_called_with("test", "alias")

    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_copy_archives_should_raise_exception(self, mock_func, mock_subpr):
        context = {"nameNode": "hdfs://namenode:5000/"}

        mock_subpr.return_value.run_command.return_value.exit_code = 1
        mock_func.return_value = "hdfs://test", "alias"
        copy_to_worker = CopyToWorker(archives=["test"])
        with self.assertRaisesRegex(AirflowException, "Copy archives to worker Failed."):
            copy_to_worker.copy_archives_to_worker(context)

    @mock.patch.object(CopyToWorker, "copy_archives_to_worker", autospec=True)
    @mock.patch.object(CopyToWorker, "copy_files_to_worker", autospec=True)
    def test_copy_all_to_worker_should_copy_files_when_files(self, mock_files, mock_archives):
        context = {"nameNode": "hdfs://namenode:5000/"}

        copy = CopyToWorker(files=["test"])
        copy.copy_all_to_worker(context)
        mock_files.assert_called_once()
        mock_archives.assert_not_called()

    @mock.patch.object(CopyToWorker, "copy_archives_to_worker", autospec=True)
    @mock.patch.object(CopyToWorker, "copy_files_to_worker", autospec=True)
    def test_copy_all_to_worker_should_copy_archives_when_archives(self, mock_files, mock_archives):
        context = {"nameNode": "hdfs://namenode:5000/"}

        copy = CopyToWorker(archives=["test"])
        copy.copy_all_to_worker(context)
        mock_archives.assert_called_once()
        mock_files.assert_not_called()

    @mock.patch.object(CopyToWorker, "copy_archives_to_worker", autospec=True)
    @mock.patch.object(CopyToWorker, "copy_files_to_worker", autospec=True)
    def test_copy_all_to_worker_should_copy_archives_and_files(self, mock_files, mock_archives):
        context = {"nameNode": "hdfs://namenode:5000/"}

        copy = CopyToWorker(archives=["test"], files=["test_files"])
        copy.copy_all_to_worker(context)
        mock_archives.assert_called_once()
        mock_files.assert_called_once()
