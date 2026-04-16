--find all records where is_active = true even though work_end_date is specified
select
    *
from {{ ref('stg__employees') }}
where is_active = true and work_end_date is not null