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
        valid_from,
        valid_to
    from valid_to_from_added

),

final as (

    select * from necessary_columns_only

)

select * from final

