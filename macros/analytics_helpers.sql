{% macro weighted_avg(column, weight, table=none) %}
    SUM({{ column }} * {{ weight }}) / NULLIF(SUM({{ weight }}), 0)
{% endmacro %}

{% macro date_diff(start_date, end_date, date_part='day') %}
    DATEDIFF('{{ date_part }}', {{ start_date }}, {{ end_date }})
{% endmacro %}

{% macro percent_of_total(column, group_by_column) %}
    {{ column }} / SUM({{ column }}) OVER (PARTITION BY {{ group_by_column }})
{% endmacro %}

{% macro rank_within_group(column, partition_by, order_by='DESC') %}
    RANK() OVER (PARTITION BY {{ partition_by }} ORDER BY {{ column }} {{ order_by }})
{% endmacro %}

