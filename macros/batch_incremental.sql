{% materialization batch_incremental, default %}

    {# config #}
    {# optional #}
    {% set incremental_strategy = config.get('incremental_strategy', default="merge") %}
    {% set batch_size = config.get('incremental_strategy', default="week") %}

    {# required #}
    {% if incremental_strategy == "merge" %}
        {% set start_run = config.require('unique_key') %}
    {% set start_run = config.require('start_run') %}
    {% set end_run = config.require('end_run') %}
    {% set date_column = config.require('date_column') %}

    {% set target_relation = this %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- begin sql
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    
    {# build sql query #}
    {% set build_sql %}
        {{ new_backfill(start_run=start_run, end_run=end_run, batch_size=batch_size, date_column=date_column, sql) }}
    {% endset %}

    {% call statement('main') %}
        {{ build_sql }}
    {% endcall %}

    {{ return({'relations': [this]}) }}

{% endmaterialization%}



{% macro batch_incremental(start_run=None, end_run=None, batch_size=None, date_column=None, sql=None) %}
    {{ print("[INFO] Running the model from " ~ start_run ~  " to " ~ end_run ~ " with a batch size of " ~ batch_size ~ ".") }}
    {{ _catch_var_edge_cases(start_run=None, end_run=None, batch_size=None, date_column=None, sql=None) }}
    {{ run_query(render(_get_merge_query(model, start_run, end_run, batch_size, date_column, sql))) }}
{% endmacro %}


{% macro _catch_var_edge_cases(model, start_run, end_run, batch_size, date_column) %}
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

    {%- if not sql -%}
        {{ exceptions.raise_compiler_error("[ERROR] The sql var is not specified in the backfill vars. Got: " ~ sql ~ ".") }}
    {%- endif -%}
{% endmacro %}