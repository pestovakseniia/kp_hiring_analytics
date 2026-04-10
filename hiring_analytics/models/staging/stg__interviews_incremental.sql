{{ 
    config(
        alias='stg_interview_incremental', 
        materialized='incremental',
        unique_key='id',
        incremental_strategy = 'merge'
    ) 
}}

with source as (

    select * 
    from {{ source('snowflake_sources', 'interviews') }}

    {% if is_incremental() %}
        where convert_timezone('America/Los_Angeles', 'UTC', to_timestamp(_updated_micros)) > (
            select max(updated_at) from {{ this }}
        )
    {% endif %}
    
),

renamed as (

    select 
        cast(_offset as bigint) as offset,
        cast(id as varchar(100)) as id,
        cast(candidate_type as varchar(50)) as candidate_type,
        cast(candidate_id as varchar(100)) as candidate_id,
        cast(status as varchar(50)) as status,
        cast(interviewer_id as varchar(100)) as interviewer_id,
        cast(location as varchar(50)) as location,
        case 
            when logged = 'true' then TRUE
            else FALSE
        end as is_logged, 
        case 
            when media_available = 'true' then TRUE
            else FALSE
        end as is_media_available,
        cast(run_type as varchar(50)) as run_type,
        cast(type as varchar(50)) as type,
        cast(media_status as varchar(50)) as media_status,
        cast(invite_answer_status as varchar(50)) as invite_answer_status,
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