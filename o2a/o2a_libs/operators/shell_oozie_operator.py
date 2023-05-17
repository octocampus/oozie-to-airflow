# -*- coding: utf-8 -*-
from airflow.operators.bash import BashOperator
from typing import List, Tuple

from o2a.o2a_libs.util_classes import PrepareAction, CopyToWorker


class ShellOozieOperator(BashOperator):
    template_fields = ("bash_command", "files", "archives", "prepare")

    def __init__(
        self,
        *,
        bash_command: str,
        files: List[str] = None,
        archives: List[str] = None,
        prepare: List[Tuple[str, str]] = None,
        **kwargs,
    ):
        super(ShellOozieOperator, self).__init__(bash_command=bash_command, **kwargs)
        self.files = files
        self.archives = archives
        self.prepare = prepare
        self.prepare_action = PrepareAction(prepare=self.prepare)
        self.copy_to_worker = CopyToWorker(files=files, archives=archives)

    def execute(self, context):
        self.prepare_action.prepare()
        self.copy_to_worker.copy_all_to_worker()
        super().execute(context)
