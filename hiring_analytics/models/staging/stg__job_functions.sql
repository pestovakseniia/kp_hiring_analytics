{{ config(alias='stg_job_function', materialized='view') }}

with source as (

    select * from {{ source('raw', 'job_functions') }}
    
),

renamed as (

    select 
        cast(_offset as bigint) as offset,
        cast(job_function_id as varchar(50)) as id,
        cast(base_name as varchar(50)) as base_name,
        cast(category as varchar(50)) as category,
        case
            when is_active = 'true' then TRUE
            else FALSE
        end as is_active,
        cast(level as integer) as level,
        cast(track as varchar(1)) as track,
        cast(seniority_level as varchar(2)) as seniority_level,
        cast(seniority_index as integer) as seniority_index,
        convert_timezone('America/Los_Angeles', 'UTC', to_timestamp(_created_micros)) as created_at,
        convert_timezone('America/Los_Angeles', 'UTC', to_timestamp(_updated_micros)) as updated_at
    from source

),

deduplicated as (

    select *
    from renamed
    qualify row_number() over (partition by id order by updated_at desc) = 1

),

final as (

    select * from deduplicated

)    

select * from final