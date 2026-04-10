{{ 
    config(
        alias='stg_interview_latest' 
    ) 
}}

{{ latest_staging_model('stg__interviews_historical') }}