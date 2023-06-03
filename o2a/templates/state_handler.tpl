{{ task | to_var }} = BranchPythonOperator(
        task_id = {{ task | to_python }},
        python_callable = get_upstream_state,
        op_kwargs={"task_id" : {{upstream | to_python}}, "dag_id" : {{dag_id | to_python}}, "ok" : {{ok | to_python}}, "error" : {{ error | to_python}}},
        provide_context= True,
        trigger_rule = "all_done"
    )
