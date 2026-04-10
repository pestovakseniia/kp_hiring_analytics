{% macro staging_model(raw_table_name, unique_key) %}
    {% set mapping_query %}
        select
            raw_column_name,
            target_column_name,
            target_data_type,
            target_order_num
        from {{ ref('stg__column_mapping') }}
        where raw_table_name = '{{ raw_table_name }}'
        order by target_order_num
    {% endset %}

    {% set mapping_result = run_query(mapping_query) %}

    {% if execute %}
        {% set rows = mapping_result.rows %}
    {% else %}
        {% set rows = [] %}
    {% endif %}

    with source as (
        select * from {{ source('snowflake_sources', raw_table_name | lower) }}
    ),

    renamed_and_type_casted as (
        select 
        {% for row in rows %}
            cast({{ row[0] }} as {{ row[2] }}) as {{ row[1] }}
            {% if not loop.last %}, {% endif %}
        {% endfor%}
        
        from source
    ),

    valid_to_from_added as (
        select *,
            updated_date as valid_from,
            case 
                when lead(updated_date) over (partition by {{ unique_key }} order by updated_date) is NULL then '9999-12-31 12:59:59.999'
                else lead(updated_date) over (partition by {{ unique_key }} order by updated_date)
            end as valid_to
        from renamed_and_type_casted
    ),

    valid_flag_added as (
        select *,
            case when valid_to = '9999-12-31 12:59:59.999' then 1 else 0
            end as row_is_active
        from valid_to_from_added
    ),

    final as (
        select * from valid_flag_added
    )

    select * from final

{% endmacro %}

-- {% macro latest_staging_model() %}
    
-- {% endmacro %}