{% test no_whitespace(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
WHERE 
    LENGTH({{ column_name }}) != LENGTH(TRIM({{ column_name }}))

{% endtest %}