# -*- coding: utf-8 -*-
from typing import Optional, List, Tuple

from airflow.providers.apache.hive.operators.hive import HiveOperator
from o2a.o2a_libs.util_classes import PrepareAction, CopyToWorker


class HiveOozieOperator(HiveOperator):
    template_fields = (*HiveOperator.template_fields, "files", "archives", "prepare")

    def __init__(
        self,
        *,
        files: Optional[List[str]] = None,
        archives: Optional[List[str]] = None,
        prepare: Optional[List[Tuple[str, str]]] = None,
        **kwargs,
    ):
        super(HiveOozieOperator, self).__init__(**kwargs)
        self.files = files
        self.archives = archives
        self.prepare = prepare
        self.prepare_action = PrepareAction(prepare=self.prepare)
        self.copy_to_worker = CopyToWorker(files=files, archives=archives)

    def execute(self, context) -> None:
        self.prepare_action.prepare(context.get("params"))
        self.copy_to_worker.copy_all_to_worker(context.get("params"))
        super().execute(context)
