CREATE TABLE raw_streaming_events (
    event_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Store Kafka metadata as regular columns for tracking
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,

    -- Payload data
    track_id VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    platform VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,

    -- Unique constraint to prevent duplicate processing of the same message
    CONSTRAINT unique_kafka_message UNIQUE (kafka_partition, kafka_offset)
);
