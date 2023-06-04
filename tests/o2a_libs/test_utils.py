# -*- coding: utf-8 -*-
import subprocess
import unittest
from unittest import mock
from unittest.mock import mock_open

from airflow import AirflowException

from o2a.o2a_libs.utils import handle_archive, construct_hdfs_path, construct_hdfs_path_and_alias


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

    @mock.patch("o2a.o2a_libs.utils.zipfile")
    def test_handle_archive_should_unzip_zip_file(self, mock_zip):
        handle_archive("test.zip", "alias")
        mock_zip.ZipFile.assert_called_with("test.zip", "r")

    @mock.patch("o2a.o2a_libs.utils.tarfile")
    def test_handle_archive_should_decompress_targz_file(self, mock_tar):
        handle_archive("test.tar.gz", "alias")
        mock_tar.open.assert_called_with("test.tar.gz", "r:gz")

    @mock.patch("o2a.o2a_libs.utils.tarfile")
    def test_handle_archive_should_decompress_tar_file(self, mock_tar):
        handle_archive("test.tar", "alias")
        mock_tar.open.assert_called_with("test.tar", "r:")

    @mock.patch("builtins.open", new_callable=mock_open)
    @mock.patch("o2a.o2a_libs.utils.gzip")
    @mock.patch("o2a.o2a_libs.utils.shutil")
    def test_handle_archive_should_decompress_gz_files(self, mock_shutil, mock_gz, mock_file):
        handle_archive("test.gz", "alias")
        mock_gz.open.assert_called_with("test.gz", "rb")
        mock_file.assert_called_with("alias", "wb")
        mock_shutil.copyfileobj.assert_called_once()


class TestConstructHdfsPath(unittest.TestCase):
    def test_construct_hdfs_path_should_return_path_if_path_starts_with_hdfs(self):
        path = "hdfs://namenode/user"
        context = {}
        result = construct_hdfs_path(context, path)
        self.assertEqual(path, result)

    def test_construct_path_should_return_hdfs_path_if_path_is_absolute(self):
        path = "/user/datapath"
        context = {"nameNode": "hdfs://namenode/"}
        result = construct_hdfs_path(context, path)
        self.assertEqual("hdfs://namenode/user/datapath", result)

    def test_construct_path_should_return_hdfs_path_if_workflowAppUri_present(self):
        path = "datapath"
        context = {"workflowAppUri": "hdfs://namenode/wf"}
        result = construct_hdfs_path(context, path)
        self.assertEqual("hdfs://namenode/wf/datapath", result)

    def test_construct_path_should_return_hdfs_path_if_wf_oozie_path_present(self):
        path = "datapath"
        context = {"oozie.wf.application.path": "hdfs://namenode/wf"}
        result = construct_hdfs_path(context, path)
        self.assertEqual("hdfs://namenode/wf/datapath", result)


class TestConstructHdfsPathandAlias(unittest.TestCase):
    def test_construct_path_and_alias_should_raise_an_exception_when_multiple_alias(self):
        path = "datapath#alias1#alias2"
        context = {"workflowAppUri": "hdfs://namenode/wf"}
        with self.assertRaisesRegex(AirflowException, "Can't have more than one # in files or archives"):
            construct_hdfs_path_and_alias(context, path)

    def test_construct_path_and_alias_should_return_basename_as_alias(self):
        path = "datapath"
        context = {"workflowAppUri": "hdfs://namenode/wf"}
        result = construct_hdfs_path_and_alias(context, path)
        self.assertEqual(("hdfs://namenode/wf/datapath", "datapath"), result)

    def test_construct_path_and_alias_should_return_constructed_path_and_alias(self):
        path = "datapath#alias1"
        context = {"workflowAppUri": "hdfs://namenode/wf"}
        result = construct_hdfs_path_and_alias(context, path)
        self.assertEqual(("hdfs://namenode/wf/datapath", "alias1"), result)
