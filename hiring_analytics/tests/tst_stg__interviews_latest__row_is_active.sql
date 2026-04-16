select
    *
from {{ ref('stg__interviews_latest') }}
where row_is_active = 1 and valid_to != '9999-12-31 12:59:59.999'