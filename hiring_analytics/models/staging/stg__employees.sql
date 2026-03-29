{{ config(alias='stg_employee') }}

with source as (

    select * from {{ source('raw', 'employees') }}

),

with renamed as (

    select
        cast(_offset as bigint) as offset,
        cast(employee_id as varchar(100)) as id,
        cast(job_function_id as varchar(50)) as job_function_id,
        cast(primary_skill_id as varchar(50)) as primary_skill_id,
        cast(production_category as varchar(50)) as production_category,
        cast(employment_status as varchar(50)) as employment_status,
        cast(org_category as varchar(50)) as org_category,
        cast(org_category_type as varchar(50)) as org_category_type,
        convert_timezone('America/Los_Angeles', 'UTC', to_timestamp(work_start_micros)) as work_start_date,
        convert_timezone('America/Los_Angeles', 'UTC', to_timestamp(work_end_micros)) as work_end_date,
        case
            when is_active = 'true' then TRUE
            else FALSE
        end as is_active,
        convert_timezone('America/Los_Angeles', 'UTC', to_timestamp(_created_micros)) as created_at,
        convert_timezone('America/Los_Angeles', 'UTC', to_timestamp(_updated_micros)) as updated_at
    from source

),

deduplicated as (

    select *        
    from (
        select *, 
            row_number() over (partition by id order by updated_at desc) as rn
        from renamed
    ) _
    where _.rn = 1

),

final as (

    select * from deduplicated

)    

select * from final