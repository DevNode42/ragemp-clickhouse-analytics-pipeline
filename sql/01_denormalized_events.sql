CREATE TABLE IF NOT EXISTS player_session_telemetry (
    event_date Date DEFAULT toDate(ts),
    ts DateTime64(3, 'UTC'),
    shard_id LowCardinality(String),
    event_type LowCardinality(String),
    character_id UInt64,
    account_id UInt64,
    faction_id UInt16,
    vip_tier UInt8,
    session_seconds UInt32,
    money_delta Int64,
    balance_after Int64,
    pos_x Float32,
    pos_y Float32,
    client_fps UInt16,
    server_tick_ms Float32,
    PROJECTION proj_by_character_time (
        SELECT
            character_id,
            ts,
            event_type,
            money_delta,
            balance_after
        ORDER BY (character_id, ts)
    )
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (shard_id, event_type, toStartOfFiveMinutes(ts), character_id)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600;
