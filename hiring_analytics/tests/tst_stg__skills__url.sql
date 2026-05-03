-- select *
from {{ ref('stg__skills') }}
where 
    url is not null
    and not (
        lower(url) like 'https://%'
        or lower(url) like 'ftp://%'
        or lower(url) like 'http://%'
    )