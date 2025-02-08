select
    date(u.first_seen_at) as cohort_date,
    count(distinct u.domain_user_id) as users,
    sum(s.events_count) as total_events,
    avg(s.events_count) as avg_events_per_session,
    avg(s.unique_pages_viewed) as avg_pages_per_session,
    count(distinct s.session_id) as total_sessions,
    sum(timestamp_diff(s.session_end_at, s.session_start_at, minute))/count(distinct s.session_id) as avg_session_minutes
from {{ ref('dim_users') }} u
left join {{ ref('fct_sessions') }} s on u.domain_user_id = s.domain_user_id
group by 1
order by 1 desc
