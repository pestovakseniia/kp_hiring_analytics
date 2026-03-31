{% snapshot interviews_snapshots %}

select * from {{ source('snowflake_sources', 'interviews') }}

{% endsnapshot %}