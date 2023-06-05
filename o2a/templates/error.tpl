{{ task_id | to_var }} = python.PythonOperator(
            task_id={{ task_id | to_python}},
            python_callable= skip_if_upstream_failed,
            op_kwargs={
                "id_task": {{ task | to_python }},
                "id_dag": {{ dag | to_python }}
                },
            trigger_rule={{ trigger_rule | to_python }}
        )
