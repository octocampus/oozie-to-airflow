{{ task_id | to_var }} = python.PythonOperator(
        task_id = {{ task_id | to_python }},
        trigger_rule="all_done",
        python_callable=resolve_subwf_state_state,
        op_kwargs={"dag": dag, "taskgroup": {{ taskgroup | to_var }}}
    )