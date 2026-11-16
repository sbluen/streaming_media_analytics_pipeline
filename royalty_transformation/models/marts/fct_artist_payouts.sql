WITH events AS (
    SELECT * FROM {{ ref('stg_streaming_events') }}
),

rates AS (
    SELECT * FROM {{ ref('platform_rates') }}
),

tracks AS (
    SELECT * FROM {{ ref('tracks') }}
)

SELECT
    e.event_id,
    e.track_id,
    COALESCE(g.genre, 'Unknown') AS genre,
    e.platform,
    e.played_at,
    COALESCE(r.rate_per_stream, 0.0038) AS payout_rate,
    1 * COALESCE(r.rate_per_stream, 0.0038) AS calculated_payout
FROM events e
LEFT JOIN rates r
    ON e.platform = r.platform_name
LEFT JOIN tracks t
    ON e.track_id = t.track_id
