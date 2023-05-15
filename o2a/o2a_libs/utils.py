# -*- coding: utf-8 -*-
import gzip
import shutil
import subprocess
import tarfile
import zipfile

from airflow import AirflowException


def construct_hdfs_path_and_alias(path: str):
    return 1, 2


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
