{{ config(alias='stg_skill') }}

with source as (

    select * from {{ source('raw', 'skills') }}
    
),

renamed as (

    select 
        cast(_offset as bigint) as offset,
        cast(id as varchar(50)) as id,
        case 
            when is_active = 'true' then TRUE
            else FALSE
        end as is_active,
        case 
            when is_primary = 'true' then TRUE
            else FALSE
        end as is_primary,
        case 
            when is_key = 'true' then TRUE
            else FALSE
        end as is_key_reason,
        cast(is_key_reason as varchar(500)) as key_reason,
        cast(type as varchar(50)) as type,
        cast(name as varchar(200)) as name,
        cast(url as varchar(500)) as url,
        cast(parent_id as varchar(50)) as parent_id,
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