{% if propagate %}
    {% if override_subwf_config %}
    subwf_props.update(JOB_PROPS)
    {% else %}
    JOB_PROPS.copy().update(JOB_PROPS)
    {% endif %}
{% else %}
subwf_props
{% endif %}