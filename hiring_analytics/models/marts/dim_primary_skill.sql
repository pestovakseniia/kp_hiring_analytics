with source as (

    select * from {{ ref('stg__skills') }} 

),

necessary_columns_only as (

    select 
        id,
        is_active,
        type,
        name,
        url,
        parent_id
    from source

),

final as (

    select * from necessary_columns_only

)

select * from final