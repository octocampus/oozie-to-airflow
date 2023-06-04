# -*- coding: utf-8 -*-
from airflow.models import BaseOperator
from typing import Any, List, Optional, Sequence, Tuple

from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
from airflow.settings import WEB_COLORS

from o2a.o2a_libs.util_classes import PrepareAction, CopyToWorker


class SparkOozieHook(SparkSubmitHook):
    def __init__(self, master: str, queue: Optional[str] = None, mode: Optional[str] = None, **kwargs):
        super(SparkOozieHook, self).__init__(**kwargs)
        self.master = master
        self.queue = queue
        self.mode = mode

    def _build_spark_submit_command(self, application: str) -> list[str]:
        cmd = super()._build_spark_submit_command(application)
        cmd = self._replace_command(cmd, "--master", self.master)
        cmd = self._replace_command(cmd, "--queue", self.queue)
        cmd = self._replace_command(cmd, "--deploy-mode", self.mode)
        return cmd

    def _replace_command(self, cmd_list: List[str], option: str, replace: str):
        for index in range(len(cmd_list)):
            if cmd_list[index] == option:
                cmd_list[index + 1] = replace
        return cmd_list


class SparkOozieOperator(BaseOperator):
    """
    This hook is a wrapper around the spark-submit binary to kick off a spark-submit job.
    It requires that the "spark-submit" binary is in the PATH.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SparkSubmitOperator`

    :param application: The application that submitted as a job, either jar or py file. (templated)
    :param conf: Arbitrary Spark configuration properties (templated)
    :param spark_conn_id: The :ref:`spark connection id <howto/connection:spark>` as configured
        in Airflow administration. When an invalid connection_id is supplied, it will default to yarn.
    :param files: Upload additional files to the executor running the job, separated by a
                  comma. Files will be placed in the working directory of each executor.
                  For example, serialized objects. (templated)
    :param py_files: Additional python files used by the job, can be .zip, .egg or .py. (templated)
    :param jars: Submit additional jars to upload and place them in executor classpath. (templated)
    :param driver_class_path: Additional, driver-specific, classpath settings. (templated)
    :param java_class: the main class of the Java application
    :param packages: Comma-separated list of maven coordinates of jars to include on the
                     driver and executor classpaths. (templated)
    :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
                             while resolving the dependencies provided in 'packages' (templated)
    :param repositories: Comma-separated list of additional remote repositories to search
                         for the maven coordinates given with 'packages'
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
                                 (Default: all the available cores on the worker)
    :param executor_cores: (Standalone & YARN only) Number of cores per executor (Default: 2)
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
    :param keytab: Full path to the file that contains the keytab (templated)
    :param principal: The name of the kerberos principal used for keytab (templated)
    :param proxy_user: User to impersonate when submitting the application (templated)
    :param name: Name of the job (default airflow-spark). (templated)
    :param num_executors: Number of executors to launch
    :param status_poll_interval: Seconds to wait between polls of driver status in cluster
        mode (Default: 1)
    :param application_args: Arguments for the application being submitted (templated)
    :param env_vars: Environment variables for spark-submit. It supports yarn and k8s mode too. (templated)
    :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
    :param spark_binary: The command to use for spark submit.
                         Some distros may use spark2-submit or spark3-submit.
    """

    template_fields: Sequence[str] = (
        "_application",
        "_conf",
        "_files",
        "_py_files",
        "_jars",
        "_driver_class_path",
        "_packages",
        "_exclude_packages",
        "_keytab",
        "_principal",
        "_proxy_user",
        "_name",
        "_application_args",
        "_env_vars",
    )
    ui_color = WEB_COLORS["LIGHTORANGE"]

    def __init__(
        self,
        *,
        master: str,
        queue: Optional[str] = None,
        mode: Optional[str] = None,
        application: str = "",
        conf: Optional[dict[str, Any]] = None,
        conn_id: Optional[str] = "spark_default",
        prepare: Optional[List[Tuple[str, str]]] = None,
        files: Optional[str] = None,
        oozie_files: Optional[List[str]] = None,
        py_files: Optional[str] = None,
        archives: Optional[str] = None,
        oozie_archives: Optional[List[str]] = None,
        driver_class_path: Optional[str] = None,
        jars: Optional[str] = None,
        java_class: Optional[str] = None,
        packages: Optional[str] = None,
        exclude_packages: Optional[str] = None,
        repositories: Optional[str] = None,
        total_executor_cores: Optional[int] = None,
        executor_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
        driver_memory: Optional[str] = None,
        keytab: Optional[str] = None,
        principal: Optional[str] = None,
        proxy_user: Optional[str] = None,
        name: str = "arrow-spark",
        num_executors: Optional[int] = None,
        status_poll_interval: int = 1,
        application_args: Optional[list[Any]] = None,
        env_vars: Optional[dict[str, Any]] = None,
        verbose: bool = False,
        spark_binary: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.oozie_files = oozie_files
        self.oozie_archives = oozie_archives
        self.prepare = prepare
        self.prepare_action = PrepareAction(prepare=self.prepare)
        self.copy_to_worker = CopyToWorker(files=files, archives=archives)
        self._application = application
        self._master = master
        self._queue = queue
        self._mode = mode
        self._conf = conf
        self._files = files
        self._py_files = py_files
        self._archives = archives
        self._driver_class_path = driver_class_path
        self._jars = jars
        self._java_class = java_class
        self._packages = packages
        self._exclude_packages = exclude_packages
        self._repositories = repositories
        self._total_executor_cores = total_executor_cores
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._keytab = keytab
        self._principal = principal
        self._proxy_user = proxy_user
        self._name = name
        self._num_executors = num_executors
        self._status_poll_interval = status_poll_interval
        self._application_args = application_args
        self._env_vars = env_vars
        self._verbose = verbose
        self._spark_binary = spark_binary
        self._hook: Optional[SparkOozieHook] = None
        self._conn_id = conn_id

    def execute(self, context) -> None:
        """Call the SparkSubmitHook to run the provided spark job"""
        self.prepare_action.prepare(context.get("params"))
        self.copy_to_worker.copy_all_to_worker(context.get("params"))
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.submit(self._application)

    def on_kill(self) -> None:
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.on_kill()

    def _get_hook(self) -> SparkSubmitHook:
        return SparkOozieHook(
            master=self._master,
            mode=self._mode,
            queue=self._queue,
            conf=self._conf,
            conn_id=self._conn_id,
            files=self._files,
            py_files=self._py_files,
            archives=self._archives,
            driver_class_path=self._driver_class_path,
            jars=self._jars,
            java_class=self._java_class,
            packages=self._packages,
            exclude_packages=self._exclude_packages,
            repositories=self._repositories,
            total_executor_cores=self._total_executor_cores,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            keytab=self._keytab,
            principal=self._principal,
            proxy_user=self._proxy_user,
            name=self._name,
            num_executors=self._num_executors,
            status_poll_interval=self._status_poll_interval,
            application_args=self._application_args,
            env_vars=self._env_vars,
            verbose=self._verbose,
            spark_binary=self._spark_binary,
        )
