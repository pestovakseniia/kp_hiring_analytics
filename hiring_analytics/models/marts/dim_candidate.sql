with source as (

    select * from {{ ref('stg__candidates') }}

),

valid_to_from_added as (
    {{ add_valid_from_and_to('source', 'ID') }}
),

necessary_columns_only as (

    select 
        offset,
        id,
        primary_skill_id,
        staffing_status,
        english_level,
        job_function_id,
        valid_from as valid_from_datetime,
        valid_to as valid_to_datetime
    from valid_to_from_added

),

final as (

    select * from necessary_columns_only

)

select * from final

