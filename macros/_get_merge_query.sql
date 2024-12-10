{% macro _get_merge_query(start_run, end_run, batch_size, date_column, sql) %}
    {% set merge_queries = [] %}
    {% for event_start, event_end in _get_interval_logic(start_run=start_run, end_run=end_run, batch_size=batch_size).items() %}
        {{ merge_queries.append(_format_query_template(event_start=event_start, event_end=event_end, date_column=date_column, sql)) }}
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

{% macro _format_query_template(model, event_start, event_end, date_column, incremental_strategy, sql) %}
    {%- if incremental_strategy == "merge" -%}
        {% set merge_query_template %}
        merge with schema evolution into {{ target_relation }} as target
                using (
                    select
                        *
                    from
                        {{ sql }}
                ) as source
                on source.{{ date_column }} = target.{{ date_column }}
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