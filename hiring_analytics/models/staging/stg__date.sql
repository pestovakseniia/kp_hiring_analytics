{{ config(alias='stg_date', materialized='view') }}

with source_candidates as (

    select
        _created_micros,
        _updated_micros
    from {{ source('snowflake_sources', 'candidates') }}

),

source_employees as (

    select
        work_start_micros,
        work_end_micros,
        _created_micros,
        _updated_micros
    from {{ source('snowflake_sources', 'employees') }}

),

source_job_functions as (

    select
        _created_micros,
        _updated_micros
    from {{ source('snowflake_sources', 'job_functions') }}

),

source_skills as (

    select
        _created_micros,
        _updated_micros
    from {{ source('snowflake_sources', 'skills') }}

),

source_interviews as (

    select
        _created_micros,
        _updated_micros
    from {{ source('snowflake_sources', 'interviews') }}

),

sources_united as (

    select to_date(_created_micros) as date from source_candidates
    union
    select to_date(_updated_micros) from source_candidates
    union
    select to_date(work_start_micros) from source_employees
    union
    select to_date(work_end_micros) from source_employees
    union
    select to_date(_created_micros) from source_employees
    union
    select to_date(_updated_micros) from source_employees
    union
    select to_date(_created_micros) from source_job_functions
    union
    select to_date(_updated_micros) from source_job_functions
    union
    select to_date(_created_micros) from source_skills
    union
    select to_date(_updated_micros) from source_skills
    union
    select to_date(_created_micros) from source_interviews
    union
    select to_date(_updated_micros) from source_interviews

),

enriched as (

    select
        date,
        year(date) as year,
        quarter(date) as quarter,
        month(date) as month,
        day(date) as day,
        week(date) as week,
        dayofweek(date) as day_of_week,
        dayname(date) as day_name,
        monthname(date) as month_name,
        not coalesce (day_of_week between 1 and 5, false) as is_weekend,
        case
            when month = 1 and day = 1 then TRUE --New Year’s Day
            when month = 7 and day = 4 then TRUE --Independence Day
            when month = 11 and day = 11 then TRUE --Veterans Day
            when month = 12 and day = 25 then TRUE --Christmas Day
            when month = 1 and day_of_week = 1 and day between 15 and 21 then TRUE --MLK
            when month = 2 and day_of_week = 1 and day between 15 and 21 then TRUE --Presidents’ Day
            when month = 5 and day_of_week = 1 and day + 7 > 31 then TRUE --Memorial Day
            when month = 9 and day_of_week = 1 and day <= 7 then TRUE --Labor Day
            when month = 10 and day_of_week = 1 and day between 8 and 14 then TRUE --Columbus Day
            when month = 11 and day_of_week = 4 and day between 22 and 28 then TRUE --Thanksgiving
            else FALSE
        end as is_holiday
    from sources_united

),

typecasted as (

    select
        date,
        cast(year as integer) as year,
        cast(quarter as integer) as quarter,
        cast(month as integer) as month,
        cast(day as integer) as day,
        cast(week as integer) as week,
        cast(day_of_week as integer) as day_of_week,
        cast(day_name as varchar(10)) as day_name,
        cast(month_name as varchar(10)) as month_name,
        is_weekend,
        is_holiday
    from enriched

),

final as (

    select * from typecasted

)

select * from final