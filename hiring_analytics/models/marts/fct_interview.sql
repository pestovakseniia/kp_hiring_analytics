with source_interviews as (

    select * from {{ ref('stg__interviews_historical') }}
    
),

source_candidates as (

    select 
        offset,
        id 
    from {{ ref('stg__candidates') }}

),

source_employees as (

    select * from {{ ref('stg__employees') }}

),

interviews_joined_candidates as (

    select i.*, c.offset as candidate_offset 
    from source_interviews as i
    left join source_candidates as c on i.candidate_id = c.id

),

interviews_joined_cemployees as (

    select i.*, e.offset as interviewer_offset 
    from interviews_joined_candidates as i
    left join source_employees as e on i.interviewer_id = e.id

),

normalized as (

    select
        id,
        candidate_type,
        candidate_offset,
        upper(regexp_replace(status, '^[^A-Za-z]+|[^A-Za-z]+$', '')) as status,
        interviewer_offset,
        location,
        is_logged,
        is_media_available,
        run_type,
        type,
        media_status,
        invite_answer_status,
        to_date(created_at) as created_date,
        created_at as created_datetime,
        valid_from
    from interviews_joined_cemployees

),

datetimes_added as (

    select *,
        min(case when status = 'REQUESTED' then valid_from end) over (partition by id) as requested_datetime,
        min(case when status = 'SCHEDULED' then valid_from end) over (partition by id) as scheduled_datetime,
        min(case when status = 'IN_PROGRESS' then valid_from end) over (partition by id) as started_datetime,
        min(case when status = 'COMPLETED' then valid_from end) over (partition by id) as finished_datetime,
        min(case when status = 'PENDING_FEEDBACK' then valid_from end) over (partition by id) as feedback_provided_datetime,
        min(case when status = 'CANCELLED' then valid_from end) over (partition by id) as cancelled_datetime
    from normalized

),

durations_added as (

    select *, 
        DATEDIFF(hh, started_datetime, feedback_provided_datetime) as interview_duration_hours,
        DATEDIFF(day, feedback_provided_datetime, finished_datetime) as feedback_delay_days
    from datetimes_added

),

final as (

    select * exclude valid_from from durations_added

)

select * from final