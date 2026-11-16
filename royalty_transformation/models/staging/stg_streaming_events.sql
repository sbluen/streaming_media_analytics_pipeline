SELECT
    event_id,
    kafka_partition,
    kafka_offset,
    track_id,
    user_id,
    platform,
    created_at AS played_at
FROM {{ source('raw_streaming', 'raw_streaming_events') }}
