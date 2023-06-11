{% if propagate %}
    {% if override_subwf_config %}
    subwf_config.update(CONFIG)
    {% else %}
    CONFIG.copy().update(subwf_config)
    {% endif %}
{% else %}
subwf_config
{% endif %}