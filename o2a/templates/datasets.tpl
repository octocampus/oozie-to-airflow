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




{% if datasets %}

DATASETS = [
    {% for dataset in datasets %}
    Dataset(
        name={{ dataset.name | to_python }},
        frequency={{ dataset.frequency | to_python }},
        initial_instance={{ dataset.initial_instance | to_python }},
        timezone={{ dataset.timezone | to_python }},
        uri_template={{ dataset.uri_template | to_python }},
        done_flag={{ dataset.done_flag | to_python }}
     ){% if loop.index < datasets|length %},
     {% endif %}
    {% endfor %}
]
{% endif %}
