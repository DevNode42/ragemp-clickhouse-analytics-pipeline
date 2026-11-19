import { performance } from 'node:perf_hooks';
import { ClickHouseBatchIngester, TelemetryEventRow } from '../ingester/batch-ingester';

const runClickHousePipelineBench = async (): Promise<void> => {
  const TOTAL_EVENTS = 100_000;
  let injectedFailuresLeft = 2;
  let totalPayloadBytes = 0;

  const ingester = new ClickHouseBatchIngester(
    {
      async insertNdjson(_table, body) {
        if (injectedFailuresLeft > 0) {
          injectedFailuresLeft--;
          throw new Error('CH_HTTP_503_TEMPORARY_OVERLOAD');
        }
        totalPayloadBytes += Buffer.byteLength(body, 'utf8');
      },
    },
    'player_session_telemetry',
    5_000,
    150_000,
    3
  );

  const eventTypes = ['ECONOMY_PAY', 'KILL_FEED', 'CRAFT_ITEM', 'QUEST_COMPLETE'] as const;

  global.gc?.();
  const memBefore = process.memoryUsage().heapUsed;
  const t0 = performance.now();

  for (let i = 0; i < TOTAL_EVENTS; i++) {
    const row: TelemetryEventRow = {
      ts: 1732000000000 + i,
      shard_id: 'ru-s1',
      event_type: eventTypes[i & 3]!,
      character_id: 10_000 + (i % 2_500),
      account_id: 5_000 + (i % 2_500),
      faction_id: i % 16,
      vip_tier: i % 4,
      session_seconds: 120 + (i % 7200),
      money_delta: i % 2 === 0 ? 1500 : -450,
      balance_after: 85_000 + (i % 50_000),
      pos_x: (i % 3000) - 1500,
      pos_y: (i % 3000) - 1500,
      client_fps: 60 + (i % 80),
      server_tick_ms: 4.2 + (i % 10) * 0.3,
    };

    ingester.enqueue(row);
  }

  await ingester.flush();
  const elapsedMs = performance.now() - t0;

  global.gc?.();
  const memAfter = process.memoryUsage().heapUsed;
  const stats = ingester.metrics;

  console.log(
    JSON.stringify(
      {
        ingestionThroughput: {
          rowsProcessed: stats.rowsIngested,
          batchesSent: stats.batchesSent,
          payloadMegabytes: Number((totalPayloadBytes / (1024 * 1024)).toFixed(2)),
          elapsedMs: Number(elapsedMs.toFixed(2)),
          rowsPerSec: Math.round((stats.rowsIngested / elapsedMs) * 1000),
          heapDeltaKb: Number(((memAfter - memBefore) / 1024).toFixed(2)),
        },
        resilienceAudit: {
          transientErrorsRecovered: stats.retriesExecuted,
          droppedEvents: stats.droppedOnOverflow,
        },
      },
      null,
      2
    )
  );
};

runClickHousePipelineBench();
