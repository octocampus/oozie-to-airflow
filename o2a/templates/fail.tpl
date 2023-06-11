{{ task_id | to_var }}= python.PythonOperator(
    task_id = {{ task_id | to_python }},
    python_callable = lambda: dag_failed_exception(),
    trigger_rule = {{ trigger_rule | to_python }}
)