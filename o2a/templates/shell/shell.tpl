
{#
  Copyright 2019 Google LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#}
{% import "macros/props.tpl" as props_macro %}
{{ task_id | to_var }} = ShellOozieOperator(
    task_id={{ task_id | to_python }},
    trigger_rule={{ trigger_rule | to_python }},
    bash_command={{ bash_command | to_python }},
    {% if env %}   env={{ env | to_python }}, {% endif %}
    {% if mkdir %}   mkdir={{ mkdir | to_python }}, {% endif %}
    {% if delete %}   delete={{ delete | to_python }}, {% endif %}
    {% if files %}   files={{ files | to_python }}, {% endif %}
    {% if archives %}   archives={{ archives | to_python }}, {% endif %}
)
