with source as (
    select *
    from {{ source('snowplow', 'raw_events') }}
),

staged as (
    select
        derived_tstamp as event_timestamp,
        dvce_created_tstamp as device_created_at,
        collector_tstamp as collected_at,
        user_id,
        domain_userid as domain_user_id,
        domain_sessionid as session_id,
        domain_sessionidx as session_index,
        event_id,
        event_name,
        event_vendor,
        event_version,
        page_url,
        page_title,
        page_urlpath as page_path,
        page_urlhost as page_host,
        geo_country,
        geo_region,
        geo_city,
        geo_timezone,
        geo_latitude,
        geo_longitude,
        mkt_medium as marketing_medium,
        mkt_source as marketing_source,
        mkt_campaign as marketing_campaign,
        platform,
        br_lang as browser_language,
        useragent as user_agent,
        br_viewwidth as browser_viewport_width,
        br_viewheight as browser_viewport_height,
        page_referrer,
        refr_medium as referrer_medium,
        refr_source as referrer_source
    from source
)

select * from staged
