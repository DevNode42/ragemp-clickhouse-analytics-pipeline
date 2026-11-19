export interface TelemetryEventRow {
  readonly ts: number;
  readonly shard_id: string;
  readonly event_type: string;
  readonly character_id: number;
  readonly account_id: number;
  readonly faction_id: number;
  readonly vip_tier: number;
  readonly session_seconds: number;
  readonly money_delta: number;
  readonly balance_after: number;
  readonly pos_x: number;
  readonly pos_y: number;
  readonly client_fps: number;
  readonly server_tick_ms: number;
}

export interface ClickHouseHttpClient {
  insertNdjson(table: string, body: string): Promise<void>;
}

export interface IngesterMetrics {
  readonly rowsIngested: number;
  readonly batchesSent: number;
  readonly retriesExecuted: number;
  readonly droppedOnOverflow: number;
}

export class ClickHouseBatchIngester {
  private readonly buffer: TelemetryEventRow[] = [];
  private readonly maxBatchSize: number;
  private readonly maxBufferCapacity: number;
  private readonly maxRetries: number;
  private activeFlush: Promise<number> | null = null;
  private rowsIngested = 0;
  private batchesSent = 0;
  private retriesExecuted = 0;
  private droppedOnOverflow = 0;

  constructor(
    private readonly client: ClickHouseHttpClient,
    private readonly tableName = 'player_session_telemetry',
    maxBatchSize = 2_500,
    maxBufferCapacity = 100_000,
    maxRetries = 3
  ) {
    this.maxBatchSize = maxBatchSize;
    this.maxBufferCapacity = maxBufferCapacity;
    this.maxRetries = maxRetries;
  }

  enqueue(row: TelemetryEventRow): boolean {
    if (this.buffer.length >= this.maxBufferCapacity) {
      this.droppedOnOverflow++;
      return false;
    }

    this.buffer.push(row);
    if (this.buffer.length >= this.maxBatchSize && !this.activeFlush) {
      void this.flush();
    }
    return true;
  }

  flush(): Promise<number> {
    if (this.activeFlush) return this.activeFlush;
    if (this.buffer.length === 0) return Promise.resolve(0);

    this.activeFlush = this.drainLoop().finally(() => {
      this.activeFlush = null;
    });

    return this.activeFlush;
  }

  private async drainLoop(): Promise<number> {
    let totalFlushed = 0;
    while (this.buffer.length > 0) {
      const chunk = this.buffer.splice(0, this.maxBatchSize);
      const ndjson = this.serializeChunk(chunk);
      await this.sendWithBackoff(ndjson);
      this.rowsIngested += chunk.length;
      this.batchesSent++;
      totalFlushed += chunk.length;
    }
    return totalFlushed;
  }

  get metrics(): IngesterMetrics {
    return {
      rowsIngested: this.rowsIngested,
      batchesSent: this.batchesSent,
      retriesExecuted: this.retriesExecuted,
      droppedOnOverflow: this.droppedOnOverflow,
    };
  }

  private serializeChunk(chunk: readonly TelemetryEventRow[]): string {
    const lines = new Array<string>(chunk.length);
    for (let i = 0; i < chunk.length; i++) {
      lines[i] = JSON.stringify(chunk[i]);
    }
    return lines.join('\n');
  }

  private async sendWithBackoff(ndjson: string): Promise<void> {
    let attempt = 0;
    while (true) {
      try {
        await this.client.insertNdjson(this.tableName, ndjson);
        return;
      } catch (err) {
        attempt++;
        if (attempt > this.maxRetries) throw err;
        this.retriesExecuted++;
        const delayMs = Math.min(200, 10 * (1 << (attempt - 1)));
        await new Promise<void>((r) => setTimeout(r, delayMs));
      }
    }
  }
}
