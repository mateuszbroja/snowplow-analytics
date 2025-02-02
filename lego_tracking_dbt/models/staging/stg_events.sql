with source as (
    select *
    from {{ source('snowplow', 'raw_events') }}
)

select 
    Core as event_id,
    Timestamps as event_timestamp,
    User as user_id,
    Session as session_id,
    Event as event_type,
    Page as page_url,
    Location as user_location,
    Marketing as marketing_source
from source
