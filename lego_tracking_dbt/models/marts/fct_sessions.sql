select
    session_id,
    domain_user_id,
    min(event_timestamp) as session_start_at,
    max(event_timestamp) as session_end_at,
    count(*) as events_count,
    count(distinct page_path) as unique_pages_viewed,
    min(marketing_source) as first_marketing_source,
    min(marketing_medium) as first_marketing_medium,
    min(marketing_campaign) as first_marketing_campaign,
    min(geo_city) as city,
    min(geo_country) as country
from {{ ref('stg_snowplow_events') }}
group by 1, 2
