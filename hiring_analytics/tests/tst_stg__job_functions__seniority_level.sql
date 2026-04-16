select seniority_level
    from {{ ref('stg__job_functions') }} 
    where concat(track, level) != seniority_level 
        or (level is null and track != seniority_level)