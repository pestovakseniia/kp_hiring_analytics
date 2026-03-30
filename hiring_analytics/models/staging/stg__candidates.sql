{{ config(alias='stg_candidate', materialized='view') }}

with source as (

    select * from {{ source('raw', 'candidates') }}
    
),

renamed as (

    select 
        cast(_offset as bigint) as offset,
        cast(candidate_id as varchar(100)) as id,
        cast(primary_skill_id as varchar(50)) as primary_skill_id,
        cast(staffing_status as varchar(50)) as staffing_status,
        cast(english_level as varchar(50)) as english_level,
        cast(job_function_id as varchar(50)) as job_function_id,
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