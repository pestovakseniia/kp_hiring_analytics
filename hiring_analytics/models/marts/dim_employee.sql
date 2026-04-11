with source as (

    select * from {{ ref('stg__employees') }}

),

valid_to_from_added as (
    {{ add_valid_from_and_to('source', 'ID') }}
),

necessary_columns_only as (

    select 
        offset,
        id,
        job_function_id,
        primary_skill_id,
        production_category,
        employment_status,
        org_category,
        org_category_type,
        work_start_date,
        work_end_date,
        is_active,
        valid_from as valid_from_datetime,
        valid_to as valid_to_datetime
    from valid_to_from_added

),

final as (

    select * from necessary_columns_only

)

select * from final