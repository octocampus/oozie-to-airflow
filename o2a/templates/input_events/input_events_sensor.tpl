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
{% if instance %}
{{ task_id | to_var }} = HdfsSensor(
            task_id={{ task_id | to_python }},
            filepath={% include 'input_events/sensor_filepath.tpl' %},
            mode={{ mode | to_python }},
            doc={{ doc | to_python }},
            poke_interval={{ poke_interval }}, # seconds
            timeout={{ timeout }},
            hdfs_conn_id={{ hdfs_conn_id | to_python }}
        )
{% endif %}

{% if start_instance_n and end_instance_n %}
input_events = []
for i in range({{ start_instance_n }} , {{ end_instance_n }}+1 ) :
    {{ task_id | to_var }} = HdfsSensor(
        task_id={{ task_id | to_python }} +"_"+str(i),
        filepath= {%raw%}f"{{{{functions.coord.current(n={i})}}}}" {%endraw%},
        mode={{ mode | to_python }},
        doc={{ doc | to_python }},
        poke_interval={{ poke_interval }}, # seconds
        timeout={{ timeout }},
        hdfs_conn_id={{ hdfs_conn_id | to_python }}
    )
    input_events.append({{ task_id | to_var }})
{{task_id | to_var}} = input_events
{% endif %}
