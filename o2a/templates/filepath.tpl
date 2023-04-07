    {% if 'current' in instance %}
{% raw %}"{{{% endraw %}{{instance}}{% raw %}}}"{% endraw %}
    {% else %}
{% raw %}"{{functions.coord.resolve_dataset_template{% endraw %}({{uri_template | to_python}},{% if instance[0] not in '0123456789' %} {{instance}} {% else %}'{{instance}}'{%endif%})}}"
    {% endif %}
