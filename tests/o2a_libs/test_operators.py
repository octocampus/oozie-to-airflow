# -*- coding: utf-8 -*-
import subprocess
import unittest
from unittest.mock import mock_open

import mock
from airflow import AirflowException

from o2a.o2a_libs.operators import handle_archive, FilesOozieOperator


class TestHandleArchive(unittest.TestCase):
    @mock.patch.object(subprocess.Popen, "communicate", autospec=True)
    def test_handle_archive_should_call_Popen_communicate_when_jar_passed(self, mock_comm):
        mock_comm.return_value = ("success", None)
        handle_archive("test.jar", "alias.jar")
        mock_comm.assert_called_once()

    @mock.patch.object(subprocess.Popen, "communicate", autospec=True)
    def test_handle_archive_should_raise_exception_when_error_executing_cmd(self, mock_comm):
        mock_comm.return_value = (None, "Error occured while executing shell cmd")
        with self.assertRaises(AirflowException):
            handle_archive("test.jar", "alias.jar")

    @mock.patch("o2a.o2a_libs.operators.zipfile")
    def test_handle_archive_should_unzip_zip_file(self, mock_zip):
        handle_archive("test.zip", "alias")
        mock_zip.ZipFile.assert_called_with("test.zip", "r")

    @mock.patch("o2a.o2a_libs.operators.tarfile")
    def test_handle_archive_should_decompress_targz_file(self, mock_tar):
        handle_archive("test.tar.gz", "alias")
        mock_tar.open.assert_called_with("test.tar.gz", "r:gz")

    @mock.patch("o2a.o2a_libs.operators.tarfile")
    def test_handle_archive_should_decompress_tar_file(self, mock_tar):
        handle_archive("test.tar", "alias")
        mock_tar.open.assert_called_with("test.tar", "r:")

    @mock.patch("builtins.open", new_callable=mock_open)
    @mock.patch("o2a.o2a_libs.operators.gzip")
    @mock.patch("o2a.o2a_libs.operators.shutil")
    def test_handle_archive_should_decompress_gz_files(self, mock_shutil, mock_gz, mock_file):
        handle_archive("test.gz", "alias")
        mock_gz.open.assert_called_with("test.gz", "rb")
        mock_file.assert_called_with("alias", "wb")
        mock_shutil.copyfileobj.assert_called_once()


context = {"nameNode": "hdfs://test:6060", "oozie.wf.application.path": "hdfs://test:6060/wf"}


class TestFilesOozieOperator(unittest.TestCase):
    @mock.patch("o2a.o2a_libs.operators.subprocess")
    def test_FilesOozieOperator_should_copy_files_to_worker(self, mock_subprocess):
        task = FilesOozieOperator(task_id="test_id", files=["test.txt"], aliases=["alias.txt"])
        mock_subprocess.Popen.return_value.communicate.return_value = ("success", None)
        task.execute(context)

        mock_subprocess.Popen.assert_called_with(
            "hadoop fs -get hdfs://test:6060/wf/test.txt . && mv test.txt alias.txt",
            stderr=mock_subprocess.PIPE,
            stdout=mock_subprocess.PIPE,
            shell=True,
        )

    @mock.patch("o2a.o2a_libs.operators.subprocess")
    def test_FilesOozieOperator_should_raise_exception_when_error_occured_on_copying(self, mock_subprocess):
        task = FilesOozieOperator(task_id="test_id", files=["test.txt"], aliases=["alias.txt"])
        mock_subprocess.Popen.return_value.communicate.return_value = (None, "error")
        with self.assertRaisesRegex(AirflowException, "Error while copying hdfs files to worker: error"):
            task.execute(context)
