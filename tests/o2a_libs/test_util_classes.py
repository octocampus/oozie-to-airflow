# -*- coding: utf-8 -*-
import shutil
import unittest

import mock
from airflow import AirflowException

from o2a.o2a_libs.util_classes import PrepareAction, CopyToWorker


class TestPrepareAction(unittest.TestCase):
    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_prepare_mkdir_should_make_directory(self, mock_func, mock_subpr):
        mock_subpr.return_value.run_command.return_value.exit_code = 0
        mock_func.return_value = "hdfs://test", ""
        prepare_action = PrepareAction(mkdir=["test"])
        prepare_action.prepare_mkdir()
        mock_func.assert_called_with("test")
        mock_subpr.return_value.run_command.assert_called_with(
            command=[shutil.which("bash"), "-c", "hadoop fs -mkdir hdfs://test"]
        )

    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_prepare_mkdir_should_raise_exception_when_mkdir_fail_creating_directory(
        self, mock_func, mock_subpr
    ):
        mock_subpr.return_value.run_command.return_value.exit_code = 1
        mock_func.return_value = "hdfs://test", ""
        prepare_action = PrepareAction(mkdir=["test"])
        with self.assertRaisesRegex(
            AirflowException, "Error occured while creating hdfs directory <prepare mkdir tag>."
        ):
            prepare_action.prepare_mkdir()
        mock_func.assert_called_with("test")

    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_prepare_delete_should_delete_directory(self, mock_func, mock_subpr):
        mock_subpr.return_value.run_command.return_value.exit_code = 0
        mock_func.return_value = "hdfs://test", ""
        prepare_action = PrepareAction(delete=["test"])
        prepare_action.prepare_delete()
        mock_func.assert_called_with("test")
        mock_subpr.return_value.run_command.assert_called_with(
            command=[shutil.which("bash"), "-c", "hadoop fs -rm -rf hdfs://test"]
        )

    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_prepare_delete_should_raise_exception_when_delete_fail_deleting_directory(
        self, mock_func, mock_subpr
    ):
        mock_subpr.return_value.run_command.return_value.exit_code = 1
        mock_func.return_value = "hdfs://test", ""
        prepare_action = PrepareAction(delete=["test"])
        with self.assertRaisesRegex(
            AirflowException, "Error occured while deleting hdfs directory <prepare delete tag>."
        ):
            prepare_action.prepare_delete()
        mock_func.assert_called_with("test")

    @mock.patch.object(PrepareAction, "prepare_delete", autospec=True)
    @mock.patch.object(PrepareAction, "prepare_mkdir", autospec=True)
    def test_prepare_should_call_prepare_mkdir_when_mkdir_list_provided(self, mock_mkdir, mock_delete):
        prepare_action = PrepareAction(mkdir=["test"])
        prepare_action.prepare()
        mock_mkdir.assert_called_once()
        mock_delete.assert_not_called()

    @mock.patch.object(PrepareAction, "prepare_delete", autospec=True)
    @mock.patch.object(PrepareAction, "prepare_mkdir", autospec=True)
    def test_prepare_should_call_prepare_delete_when_delete_list_provided(self, mock_mkdir, mock_delete):
        prepare_action = PrepareAction(delete=["test"])
        prepare_action.prepare()
        mock_delete.assert_called_once()
        mock_mkdir.assert_not_called()

    @mock.patch.object(PrepareAction, "prepare_delete", autospec=True)
    @mock.patch.object(PrepareAction, "prepare_mkdir", autospec=True)
    def test_prepare_should_call_prepare_mkdir_and_delete_when_both_lists_provided(
        self, mock_mkdir, mock_delete
    ):
        prepare_action = PrepareAction(delete=["test"], mkdir=["test_mkdir"])
        prepare_action.prepare()
        mock_delete.assert_called_once()
        mock_mkdir.assert_called_once()


class TestCopyToWorker(unittest.TestCase):
    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_copy_files_to_worker_should_copy_files(self, mock_func, mock_subpr):
        mock_subpr.return_value.run_command.return_value.exit_code = 0
        mock_func.return_value = "hdfs://test", "alias"
        copy_to_worker = CopyToWorker(files=["test"])
        copy_to_worker.copy_files_to_worker()
        mock_func.assert_called_with("test")
        mock_subpr.return_value.run_command.assert_called_with(
            command=[shutil.which("bash"), "-c", "hadoop fs -get hdfs://test . && mv test alias"]
        )

    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_copy_files_should_raise_exception_when_copying_failed(self, mock_func, mock_subpr):
        mock_subpr.return_value.run_command.return_value.exit_code = 1
        mock_func.return_value = "hdfs://test", "alias"
        copy_to_worker = CopyToWorker(files=["test"])
        with self.assertRaisesRegex(AirflowException, "Copy files to worker Failed"):
            copy_to_worker.copy_files_to_worker()
        mock_func.assert_called_with("test")

    @mock.patch("o2a.o2a_libs.util_classes.handle_archive")
    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_copy_archives_to_worker_should_copy_archives(self, mock_func, mock_subpr, mock_archive):
        mock_subpr.return_value.run_command.return_value.exit_code = 0
        mock_func.return_value = "hdfs://test", "alias"
        copy_to_worker = CopyToWorker(archives=["test"])
        copy_to_worker.copy_archives_to_worker()
        mock_func.assert_called_with("test")
        mock_subpr.return_value.run_command.assert_called_with(
            command=[shutil.which("bash"), "-c", "hadoop fs -get hdfs://test ."]
        )
        mock_archive.assert_called_with("test", "alias")

    @mock.patch("o2a.o2a_libs.util_classes.SubprocessHook")
    @mock.patch("o2a.o2a_libs.util_classes.construct_hdfs_path_and_alias")
    def test_copy_archives_should_raise_exception(self, mock_func, mock_subpr):
        mock_subpr.return_value.run_command.return_value.exit_code = 1
        mock_func.return_value = "hdfs://test", "alias"
        copy_to_worker = CopyToWorker(archives=["test"])
        with self.assertRaisesRegex(AirflowException, "Copy archives to worker Failed."):
            copy_to_worker.copy_archives_to_worker()

    @mock.patch.object(CopyToWorker, "copy_archives_to_worker", autospec=True)
    @mock.patch.object(CopyToWorker, "copy_files_to_worker", autospec=True)
    def test_copy_all_to_worker_should_copy_files_when_files(self, mock_files, mock_archives):
        copy = CopyToWorker(files=["test"])
        copy.copy_all_to_worker()
        mock_files.assert_called_once()
        mock_archives.assert_not_called()

    @mock.patch.object(CopyToWorker, "copy_archives_to_worker", autospec=True)
    @mock.patch.object(CopyToWorker, "copy_files_to_worker", autospec=True)
    def test_copy_all_to_worker_should_copy_archives_when_archives(self, mock_files, mock_archives):
        copy = CopyToWorker(archives=["test"])
        copy.copy_all_to_worker()
        mock_archives.assert_called_once()
        mock_files.assert_not_called()

    @mock.patch.object(CopyToWorker, "copy_archives_to_worker", autospec=True)
    @mock.patch.object(CopyToWorker, "copy_files_to_worker", autospec=True)
    def test_copy_all_to_worker_should_copy_archives_and_files(self, mock_files, mock_archives):
        copy = CopyToWorker(archives=["test"], files=["test_files"])
        copy.copy_all_to_worker()
        mock_archives.assert_called_once()
        mock_files.assert_called_once()
