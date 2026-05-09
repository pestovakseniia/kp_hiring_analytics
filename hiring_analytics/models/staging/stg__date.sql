{{ config(alias='stg_date', materialized='view') }}

with source_candidates as (
    select _created_micros, _updated_micros
    from {{ source('snowflake_sources', 'candidates') }}
),

source_employees as (
    select work_start_micros, work_end_micros, _created_micros, _updated_micros
    from {{ source('snowflake_sources', 'employees') }}
),

source_job_functions as (
    select _created_micros, _updated_micros
    from {{ source('snowflake_sources', 'job_functions') }}
),

source_skills as (
    select _created_micros, _updated_micros
    from {{ source('snowflake_sources', 'skills') }}
),

source_interviews as (
    select _created_micros, _updated_micros
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

min_max as (
    select
        min(date) as min_date,
        current_date() as max_date
    from sources_united
    where date is not null
),

date_spine as (
    select
        dateadd(day, seq4(), min_date) as date
    from min_max,
    table(generator(rowcount => 10000))  
    where dateadd(day, seq4(), min_date) <= max_date
),

enriched as (
    select
        d.date,
        year(d.date) as year,
        quarter(d.date) as quarter,
        month(d.date) as month,
        day(d.date) as day,
        week(d.date) as week,
        dayofweek(d.date) as day_of_week,
        dayname(d.date) as day_name,
        monthname(d.date) as month_name,

        not (dayofweek(d.date) between 1 and 5) as is_weekend,

        case
            when month(d.date) = 1 and day(d.date) = 1 then true
            when month(d.date) = 7 and day(d.date) = 4 then true
            when month(d.date) = 11 and day(d.date) = 11 then true
            when month(d.date) = 12 and day(d.date) = 25 then true
            when month(d.date) = 1 and dayofweek(d.date) = 1 and day(d.date) between 15 and 21 then true
            when month(d.date) = 2 and dayofweek(d.date) = 1 and day(d.date) between 15 and 21 then true
            when month(d.date) = 5 and dayofweek(d.date) = 1 and day(d.date) + 7 > 31 then true
            when month(d.date) = 9 and dayofweek(d.date) = 1 and day(d.date) <= 7 then true
            when month(d.date) = 10 and dayofweek(d.date) = 1 and day(d.date) between 8 and 14 then true
            when month(d.date) = 11 and dayofweek(d.date) = 4 and day(d.date) between 22 and 28 then true
            else false
        end as is_holiday

    from date_spine d
),

final as (
    select * from enriched
)

select * from final