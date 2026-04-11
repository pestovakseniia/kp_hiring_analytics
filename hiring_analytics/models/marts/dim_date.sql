with source as (

    select * from {{ ref('stg__date') }}

),

necessary_columns_only as (

    select 
        date,
        year,
        quarter,
        month,
        day,
        week,
        day_of_week,
        day_name,
        month_name,
        is_weekend,
        is_holiday
    from source

),

final as (

    select * from necessary_columns_only

)

select * from final