{% snapshot interviews_snapshot %}

select * from {{ source('snowflake_sources', 'interviews') }}

{% endsnapshot %}