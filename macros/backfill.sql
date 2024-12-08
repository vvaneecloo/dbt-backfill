{% macro backfill(model=None, start_run=None, end_run=None, batch_size=None, event_time=None, debug=False) %}
    {{ log("Running the backfill macro from" ~ start_run ~  "to" ~ end_run ~ "with a batch size of " ~ event_time}}
    {{ _catch_var_edge_cases(model, start_run, end_run, batch_size, event_time, debug) }}
    {{ do run_query(_get_merge_query(model, start_run, end_run, batch_size, event_time, debug)) }}
{% endmacro %}


{% macro _catch_edge_cases(model, start_run, end_run, batch_size, event_time, debug) %}
    {%- if not model -%}
        {{ exceptions.raise_compiler_error("[ERROR] The model var is neither specified in the backfill vars nor in the model config. Got:" ~ model ~ ".") }}
    {%- endif -%}

    {%- if not start_run -%}
        {{ exceptions.raise_compiler_error("[ERROR] The start_run var is neither specified in the backfill vars nor in the model config. Got:" ~ start_run ~ ".") }}
    {%- endif -%}

    {%- if not end_run -%}
        {{ exceptions.raise_compiler_error("[ERROR] The end_run var is neither specified in the backfill vars nor in the model config. Got:" ~ end_run ~ ".") }}
    {%- endif -%}

    {%- if not batch_size -%}
        {{ exceptions.warn("[WARN] The batch_size var is neither specified in the backfill vars nor in the model config. Got:" ~ batch_size ~ ". The backfill will begin with a 'batch_size' of 1 day.") }}
        {% set batch_size = "day" %}
    {%- endif -%}

    {%- if not event_time -%}
        {{ exceptions.raise_compiler_error("[ERROR] The event_time var is neither specified in the backfill vars nor in the model config. Got:" ~ event_time ~ ".") }}
    {%- endif -%}

    {%- if debug -%}
        {{ exceptions.raise_compiler_error("[ERROR] The event_time var is neither specified in the backfill vars nor in the model config. Got:" ~ event_time ~ ".") }}
    {%- endif -%}
{% endmacro %}