{{ task_id | to_var }} = python.PythonOperator(
            task_id={{ task_id | to_python}},
            python_callable= skip_if_upstream_failed,
            op_kwargs={
                "id_task": f"{subdag_name}." + {{ task | to_python }},
                "id_dag": dag.dag_id
                },
            trigger_rule={{ trigger_rule | to_python }}
        )
