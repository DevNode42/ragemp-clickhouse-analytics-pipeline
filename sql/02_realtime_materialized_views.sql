CREATE TABLE IF NOT EXISTS shard_realtime_kpis_agg (
    window_minute DateTime('UTC'),
    shard_id LowCardinality(String),
    event_type LowCardinality(String),
    unique_players AggregateFunction(uniq, UInt64),
    total_money_inflow SimpleAggregateFunction(sum, Int64),
    total_money_outflow SimpleAggregateFunction(sum, Int64),
    p95_server_tick_ms AggregateFunction(quantile(0.95), Float32),
    avg_client_fps AggregateFunction(avg, UInt16)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(window_minute)
ORDER BY (shard_id, window_minute, event_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS shard_realtime_kpis_mv
TO shard_realtime_kpis_agg
AS SELECT
    toStartOfMinute(ts) AS window_minute,
    shard_id,
    event_type,
    uniqState(character_id) AS unique_players,
    sumState(if(money_delta > 0, money_delta, toInt64(0))) AS total_money_inflow,
    sumState(if(money_delta < 0, abs(money_delta), toInt64(0))) AS total_money_outflow,
    quantileState(0.95)(server_tick_ms) AS p95_server_tick_ms,
    avgState(client_fps) AS avg_client_fps
FROM player_session_telemetry
GROUP BY window_minute, shard_id, event_type;
