
{{ task_id | to_var }} = operators.FilesOozieOperator(
    task_id={{ task_id | to_python }},
    files={{ files | to_python }},
    aliases= {{ aliases | to_python }}
)
