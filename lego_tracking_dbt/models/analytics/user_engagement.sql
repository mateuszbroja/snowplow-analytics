SELECT
    date_trunc(s.session_start, day) as date,
    count(distinct s.session_id) as total_sessions,
    count(distinct s.user_id) as unique_users,
    avg(s.events_count) as avg_events_per_session,
    sum(s.events_count) as total_events,
    sum(s.unique_pages_viewed) as total_pages_viewed,
    avg(timestamp_diff(s.session_end, s.session_start, minute)) as avg_session_duration_minutes
FROM {{ ref('fct_sessions') }} s
GROUP BY 1
ORDER BY 1 DESC