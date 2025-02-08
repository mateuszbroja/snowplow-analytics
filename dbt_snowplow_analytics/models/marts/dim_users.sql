with users as (
   select
       domain_user_id,
       min(event_timestamp) as first_seen_at,
       max(event_timestamp) as last_seen_at,
       count(distinct session_id) as total_sessions,
       count(distinct date(event_timestamp)) as visited_days,
       min(geo_city) as first_city,
       min(geo_country) as first_country,
       min(browser_language) as browser_language
   from {{ ref('stg_snowplow_events') }}
   group by 1
)

select * from users
