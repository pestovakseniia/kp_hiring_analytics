{% macro add_valid_from_and_to(source_cte, unique_key) %}

    select *,
        updated_at as valid_from,
        case 
            when lead(updated_at) over (partition by {{ unique_key }} order by updated_at) is NULL then '9999-12-31 12:59:59.999'
            else lead(updated_at) over (partition by {{ unique_key }} order by updated_at)
        end as valid_to
    from {{ source_cte }}

{% endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {{ custom_schema_name if custom_schema_name else target.schema }}
    
{%- endmacro %}