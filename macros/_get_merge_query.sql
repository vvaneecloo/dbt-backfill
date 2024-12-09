{% macro _get_merge_query(model, start_run, end_run, batch_size, date_column) %}
    {% set merge_queries = [] %}
    {% for event_start, event_end in _get_interval_logic(start_run=start_run, end_run=end_run, batch_size=batch_size).items() %}
        {{ merge_queries.append(_format_query_template(model=model, event_start=event_start, event_end=event_end, date_column=date_column)) }}
    {% endfor %}

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
    {%- if materialization_type == "merge" -%}
        {% set clean_code = _get_clean_code(model=model) %}
        {{ print("clean code" ~ clean_code) }}
        {% set merge_query_template %}
        merge with schema evolution into {{ ref(model) }} as target
                using (
                with source as ({{ clean_code }})
                    select
                        *
                    from
                        source
                    where
                        {{ date_column }} between date({{ event_end }}) and date({{ event_start }})
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
    {{ return(_compile(_clean_raw_code(_get_raw_code(model)))) }}
{% endmacro %}

{% macro _clean_raw_code(raw_code) %}
    {%- set lower_raw_code = raw_code | lower -%}
    {%- set code_without_config = lower_raw_code.split(")\n}}") -%}
    {%- set cleaned_code = code_without_config[1].split("{{ row_limit_for_ci() }}") -%}
    {{ return(cleaned_code[0]) }}
{% endmacro %}

{% macro _get_raw_code(model) %}
    {% if execute %}
        {% for node in graph.nodes.values()
         | selectattr("resource_type", "equalto", "model")
         | selectattr("package_name", "equalto", "insight_supply_chain")
         | selectattr("name", "equalto", model) %}
            {{ return(node.raw_code) }}
        {%- endfor -%}
    {% else %}
        {{ exceptions.raise_compiler_error("[ERROR] DBT is not executing, cannot parse graph.") }}
    {% endif %}
{% endmacro %}

{% macro _compile(raw_code) %}
    {% set re = modules.re %}
    {%- set models = re.findall('{{\s*ref\((.*?)\)\s*}}', raw_code) %}
    {%- set compiled_code = raw_code %}

    {%- for model in models -%}
        {% set compiled_ref = ref(model) %}

        {# Dynamically build the regex pattern to match `{{ ref('') }}` with the model name #}
        {% set regex_pattern = '{{\s*ref\((.*?)\)\s*}}' %}

        {# Use re.sub to replace the found `{{ ref() }}` with the compiled reference #}
        {% set compiled_code = re.sub(regex_pattern, compiled_ref | string, compiled_code) %}
        {{ print(compiled_code) }}
    {%- endfor -%}
{% endmacro %}