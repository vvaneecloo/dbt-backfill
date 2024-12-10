{% macro _get_merge_query(model, start_run, end_run, batch_size, date_column, sql) %}
    {% set merge_queries = [] %}
    {% for event_start, event_end in _get_interval_logic(start_run=start_run, end_run=end_run, batch_size=batch_size).items() %}
        {{ merge_queries.append(_format_query_template(model=model, event_start=event_start, event_end=event_end, date_column=date_column, sql)) }}
    {% endfor %}

    {{ print("".join(merge_queries)) }}

    {{ return("".join(merge_queries)) }}

{% endmacro %}

{% macro _get_interval_logic(start_run, end_run, batch_size) %}
    {% set datetime = modules.datetime %}
    {% set events = {} %}
    {% set batch_size_dict = {
        "day": 1,
        "week": 7,
        "month": 30,
        "year": 365
    } %}

    {% set step_interval = batch_size_dict.get(batch_size, 0) %}

    {% set days_diff = (start_run - end_run).days | int %}
    {% set steps = range(0, days_diff, step_interval) %}


    {% for step in steps %}
        {% set tmp_start_run = start_run - datetime.timedelta(days=step) %}
        {% set tmp_end_run = tmp_start_run - datetime.timedelta(days=step_interval) %}
        {% do events.update({tmp_start_run: tmp_end_run}) %}
    {% endfor %}
    {{ return(events) }}
{% endmacro %}

{% macro _format_query_template(model, event_start, event_end, date_column, materialization_type="merge") %}
    {{ print(_get_compiled_path(model)) }}
    {%- if materialization_type == "merge" -%}
        {% set clean_code = _get_clean_code(model=model) %}
        {% set merge_query_template %}
        merge with schema evolution into {{ ref(model) }} as target
                using (
                    select
                        *
                    from
                        {{ sql }}
                ) as source
                on source.created_at = target.created_at
                when matched then
                    update set *
                when not matched then
                    insert *
                when not matched by source then
                    delete;
        {% endset %}
    {%- endif -%}
    {{ return(merge_query_template) }}
{% endmacro %}

{% macro _get_clean_code(model) %}
    {{ return(_clean_raw_code(_get_node_infos(model).raw_code)) }}
{% endmacro %}

{% macro _clean_raw_code(raw_code) %}
    {%- set lower_raw_code = raw_code | lower -%}
    {%- set code_without_config = lower_raw_code -%}
    {% if (code_without_config.split(")\n}}") | length) > 1 %}
        {{ print(code_without_config.split(")\n}}")) }}
        {%- set code_without_config = code_without_config.split(")\n}}") -%}
        {%- set code_without_config = code_without_config[1] -%}
    {% endif %}
    {%- set cleaned_code = code_without_config.replace("{{ row_limit_for_ci() }}", "") -%}
    {{ return(cleaned_code) }}
{% endmacro %}

{% macro _get_node_infos(model) %}
    {% if execute %}
        {% for node in graph.nodes.values()
         | selectattr("resource_type", "equalto", "model")
         | selectattr("package_name", "equalto", "insight_supply_chain")
         | selectattr("name", "equalto", model) %}
            {{ return({"raw_code": node.raw_code, "refs": node.refs, "materialized": node.config.materialized, "path": node.path }) }}
        {%- endfor -%}
    {% else %}
        {{ exceptions.raise_compiler_error("[ERROR] DBT is not executing, cannot parse graph.") }}
    {% endif %}
{% endmacro %}

{% macro _get_compiled_path(model) %}
    {% set node_path = _get_node_infos(model).path %}
    {{ print("target/compiled/insight_supply_chain/models/" ~ node_path) }}
{% endmacro %}