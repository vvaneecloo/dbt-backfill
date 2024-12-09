{% macro new_backfill(model=None, start_run=None, end_run=None, batch_size="day", date_column=None) %}
    {{ _log_and_print("[INFO] Running the backfill macro from " ~ start_run ~  " to " ~ end_run ~ " with a batch size of " ~ batch_size ~ ".") }}

    {{ _catch_var_edge_cases(model, start_run, end_run, batch_size, date_column) }}
    {{ run_query(_get_merge_query(model, start_run, end_run, batch_size, date_column)) }}
{% endmacro %}


{% macro _catch_var_edge_cases(model, start_run, end_run, batch_size, date_column) %}
    {%- if not model -%}
        {{ exceptions.raise_compiler_error("[ERROR] The model var is not specified in the backfill vars. Got: " ~ model ~ ".") }}
    {%- endif -%}

    {%- if not start_run -%}
        {{ exceptions.raise_compiler_error("[ERROR] The start_run var is not specified in the backfill vars. Got: " ~ start_run ~ ".") }}
    {%- endif -%}

    {%- if not end_run -%}
        {{ exceptions.raise_compiler_error("[ERROR] The end_run var is not specified in the backfill vars. Got: " ~ end_run ~ ".") }}
    {%- endif -%}

    {%- if not batch_size -%}
        {{ exceptions.warn("[WARN] The batch_size var is not specified in the backfill vars. Got: " ~ batch_size ~ ". The backfill will begin with a 'batch_size' of 1 day.") }}
    {%- endif -%}

    {%- if not date_column -%}
        {{ exceptions.raise_compiler_error("[ERROR] The date_column var is not specified in the backfill vars. Got: " ~ date_column ~ ".") }}
    {%- endif -%}
{% endmacro %}

{% macro _log_and_print(message) %}
    {{ log(message) }}
    {{ print(message) }}
{% endmacro %}