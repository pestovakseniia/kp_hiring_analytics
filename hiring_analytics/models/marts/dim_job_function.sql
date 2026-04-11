with source as (

    select * from {{ ref('stg__job_functions') }}

),

necessary_columns_only as (

    select 
        id,
        base_name,
        category,
        is_active,
        level,
        track,
        seniority_level,
        seniority_index
    from source

),

final as (

    select * from necessary_columns_only

)

select * from final