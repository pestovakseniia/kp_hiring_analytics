with source as (

    select * from {{ ref('stg__interviews_historical') }}
    
)