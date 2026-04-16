select base_name, count(distinct category)
    from {{ ref('stg__job_functions')}}
    group by base_name
    having count(distinct category) > 1