

{{ task_id | to_var }} = operators.ArchivesOozieOperator(
    task_id={{ task_id | to_python }},
    archives={{ archives | to_python }},
    aliases= {{ aliases | to_python }}
)
