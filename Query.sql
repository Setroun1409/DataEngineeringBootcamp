-- Calculate average web events per session for each host
WITH session_data AS (
    SELECT 
        host, 
        COUNT(*) AS total_hits, 
        COUNT(DISTINCT session_id) AS total_sessions
    FROM processed_events_aggregated_source
    GROUP BY host
)
SELECT 
    host, 
    total_hits / NULLIF(total_sessions, 0) AS avg_hits_per_session
FROM session_data;
