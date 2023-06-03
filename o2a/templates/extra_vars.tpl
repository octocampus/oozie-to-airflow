{% if prepare %} prepare={{prepare | to_python}}, {% endif %}
{% if files %} files={{files | to_python}}, {% endif %}
{% if archives %} archives={{archives | to_python}}, {% endif %}
