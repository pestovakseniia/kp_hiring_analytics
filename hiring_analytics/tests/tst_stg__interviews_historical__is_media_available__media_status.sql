-- tests/stg_interviews__invalid_media_flag.sql

with normalized as (

    select
        *,
        upper(regexp_replace(status, '[^A-Z]', '')) as normalized_status
    from {{ ref('stg__interviews_historical') }}

)

select *
from normalized
where 
    normalized_status not in (
        'COMPLETED',
        'CANCELLED',
        'PENDINGFEEDBACK'
    )
    and media_status = 'DONE'