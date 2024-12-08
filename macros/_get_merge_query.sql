{% macro _get_merge_query(model, start_run, end_run, batch_size, event_time) %}
    {% set merge_query = "" %}
    {% for event_start, event_end in _get_interval_logic(start_run=start_run, end_run=end_run, event_time=event_time).items() %}
        {% set merge_query = merge_query ~ _format_query_template(model=model, event_start=event_start, event_end=event_end, event_time=event_time) %}
    {% endfor %}

    {{ return(merge_query) }}
{% endmacro %}

{% macro _get_interval_logic(start_run, end_run, batch_size, event_time) %}
    {% set datetime = modules.datetime.datetime %}
    {% set event_time_dict = {
        "day": 1,
        "week": 7,
        "month": 30,
        "year": 365
    } %}

    {% set start_run_date = datetime.date(start_run) %}
    {% set end_run_date = datetime.date(end_run) %}

    {% set events = {} %}
    {% set tmp_start_run = start_run_date %}
   
    {% while tmp_start_run > end_run_date %}
        {% set tmp_end_run = tmp_start_run - datetime.timedelta(days=event_time_dict[event_time]) %}
        {% set events = events | merge({tmp_start_run: tmp_end_run}) %}
        {% set tmp_start_run = tmp_end_run %}
    {% endwhile %}

    {{ return(events) }}
{% endmacro %}


{% macro _format_query_template(model, event_start, event_end, event_time) %}
    {% set merge_query_template %}
        merge with schema evolution into {{ target }}.{{ model }}
        using (
            select
                *
            from
                {{ ref(model)}}
            where
                {{ event_time }} is between {{ event_start }} and {{ event_end }}
        )
        on source.event_time = target.event_time
        when matched then
            update set *
        when not matched then
            update set *
        when not matched by source then
            delete;
    {% endset %}
    {{ do return(merge_query_template) }}
{% endmacro %}