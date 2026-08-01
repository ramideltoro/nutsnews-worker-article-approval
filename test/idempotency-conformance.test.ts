import type { Pool } from "pg";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import {
  APPROVAL_IDEMPOTENCY_LEASE_SECONDS,
  PostgresApprovalStateStore
} from "../src/production.js";
import {
  InMemoryApprovalStateStore,
  ManualApprovalClock,
  createMinimalApprovalEnvelope
} from "../src/test-doubles.js";

const RECEIVED_AT = "2026-08-01T12:00:00.000Z";

describe("Runtime 1.0 idempotency conformance", () => {
  it("rejects wrong-token mutations and preserves completed in-memory claims", async () => {
    const store = new InMemoryApprovalStateStore(new ManualApprovalClock(RECEIVED_AT));
    const envelope = createMinimalApprovalEnvelope();
    const claimed = await store.claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    });

    expect(claimed.status).toBe("claimed");
    if (claimed.status !== "claimed") {
      throw new Error("Expected a claimed idempotency lease.");
    }

    await expect(store.markCompleted(envelope.idempotencyKey, completion("wrong-token"))).rejects.toThrow(
      "owned by another delivery"
    );
    await expect(store.releaseClaim(envelope.idempotencyKey, failure("wrong-token"))).resolves.toEqual({
      status: "not-owned"
    });

    await expect(store.markCompleted(envelope.idempotencyKey, completion(claimed.claimToken))).resolves.toBeUndefined();
    await expect(store.releaseClaim(envelope.idempotencyKey, failure(claimed.claimToken))).resolves.toEqual({
      status: "preserved-completed"
    });
    await expect(store.claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    })).resolves.toMatchObject({
      status: "already-completed"
    });
  });

  it("releases only the owned in-memory lease and issues a new token on replay", async () => {
    const store = new InMemoryApprovalStateStore(new ManualApprovalClock(RECEIVED_AT));
    const envelope = createMinimalApprovalEnvelope();
    const first = await store.claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    });

    if (first.status !== "claimed") {
      throw new Error("Expected a claimed idempotency lease.");
    }

    await expect(store.releaseClaim(envelope.idempotencyKey, failure(first.claimToken))).resolves.toEqual({
      status: "released"
    });
    const replay = await store.claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    });

    expect(replay).toMatchObject({
      status: "claimed",
      replay: true
    });
    if (replay.status !== "claimed") {
      throw new Error("Expected a replay idempotency lease.");
    }
    expect(replay.claimToken).not.toBe(first.claimToken);
  });

  it("persists a database-timed PostgreSQL lease with every opaque claim token", async () => {
    const database = scriptedPool([
      result(1, [
        {
          received_at: new Date(RECEIVED_AT)
        }
      ])
    ]);
    const store = new PostgresApprovalStateStore(database.pool);
    const envelope = createMinimalApprovalEnvelope();
    const claimed = await store.claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    });

    expect(claimed).toMatchObject({
      status: "claimed",
      firstSeenAt: RECEIVED_AT,
      replay: false
    });
    if (claimed.status !== "claimed") {
      throw new Error("Expected a claimed PostgreSQL idempotency lease.");
    }
    expect(claimed.claimToken).toMatch(/^[0-9a-f-]{36}$/u);
    const metadata = jsonArgument(database.calls[0], 13);
    expect(metadata).toMatchObject({
      claimMessageId: envelope.messageId
    });
    expect(database.calls[0]?.values?.[14]).toBe(claimed.claimToken);
    expect(database.calls[0]?.sql).toContain("'claimedAt', statement_timestamp()");
    expect(database.calls[0]?.sql).toContain("'idempotencyLeaseAcquiredAtEpochSeconds', extract(epoch FROM statement_timestamp())");
    expect(APPROVAL_IDEMPOTENCY_LEASE_SECONDS).toBeGreaterThan(0);
    expect(APPROVAL_IDEMPOTENCY_LEASE_SECONDS).toBeLessThanOrEqual(5 * 60);
  });

  it("reclaims failed PostgreSQL work with an atomic status CAS and a fresh token", async () => {
    const firstSeenAt = new Date("2026-08-01T11:55:00.000Z");
    const database = scriptedPool([
      result(0),
      result(1, [
        {
          status: "failed",
          received_at: firstSeenAt,
          processed_at: null
        }
      ]),
      result(1, [
        {
          received_at: firstSeenAt
        }
      ])
    ]);
    const store = new PostgresApprovalStateStore(database.pool);
    const envelope = createMinimalApprovalEnvelope();
    const claimed = await store.claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    });

    expect(claimed).toMatchObject({
      status: "claimed",
      firstSeenAt: firstSeenAt.toISOString(),
      replay: true
    });
    if (claimed.status !== "claimed") {
      throw new Error("Expected a reclaimed PostgreSQL idempotency lease.");
    }
    expect(database.calls[2]?.sql).toContain("AND status IN ('failed', 'parked')");
    expect(jsonArgument(database.calls[2], 1)).toMatchObject({
      replayMessageId: envelope.messageId
    });
    expect(database.calls[2]?.values?.[2]).toBe(claimed.claimToken);
    expect(database.calls[2]?.sql).toContain("'idempotencyLeaseAcquiredAtEpochSeconds', extract(epoch FROM statement_timestamp())");
  });

  it("atomically reclaims an expired PostgreSQL lease with a fresh token within five minutes", async () => {
    const firstSeenAt = new Date("2026-08-01T11:50:00.000Z");
    const database = scriptedPool([
      result(0),
      result(1, [
        {
          status: "processing",
          received_at: firstSeenAt,
          processed_at: null,
          claim_token: "prior-claim-token",
          lease_acquired_at_epoch_seconds: "1785585000.125"
        }
      ]),
      result(1, [
        {
          received_at: firstSeenAt
        }
      ])
    ]);
    const store = new PostgresApprovalStateStore(database.pool);
    const envelope = createMinimalApprovalEnvelope();
    const reclaimed = await store.claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    });

    expect(reclaimed).toMatchObject({
      status: "claimed",
      firstSeenAt: firstSeenAt.toISOString(),
      replay: true
    });
    if (reclaimed.status !== "claimed") {
      throw new Error("Expected an expired PostgreSQL lease to be reclaimed.");
    }
    expect(reclaimed.claimToken).not.toBe("prior-claim-token");
    const reclaim = database.calls[2];
    expect(reclaim?.sql).toContain("AND status = 'processing'");
    expect(reclaim?.sql).toContain("diagnostic_metadata->>'idempotencyClaimToken' = $4");
    expect(reclaim?.sql).toContain("diagnostic_metadata->>'idempotencyLeaseAcquiredAtEpochSeconds' = $5");
    expect(reclaim?.sql).toContain("jsonb_typeof(diagnostic_metadata->'idempotencyLeaseAcquiredAtEpochSeconds') = 'number'");
    expect(reclaim?.sql).toContain("extract(epoch FROM statement_timestamp()) - $6::numeric");
    expect(reclaim?.values?.[2]).toBe(reclaimed.claimToken);
    expect(reclaim?.values?.[3]).toBe("prior-claim-token");
    expect(reclaim?.values?.[4]).toBe("1785585000.125");
    expect(reclaim?.values?.[5]).toBe(APPROVAL_IDEMPOTENCY_LEASE_SECONDS);
  });

  it("does not reclaim a fresh, foreign, malformed, or concurrently completed PostgreSQL lease", async () => {
    const firstSeenAt = new Date("2026-08-01T11:59:00.000Z");
    const contestedDatabase = scriptedPool([
      result(0),
      result(1, [
        {
          status: "processing",
          received_at: firstSeenAt,
          processed_at: null,
          claim_token: "selected-token",
          lease_acquired_at_epoch_seconds: "1785585540.000"
        }
      ]),
      result(0)
    ]);
    const envelope = createMinimalApprovalEnvelope();

    await expect(new PostgresApprovalStateStore(contestedDatabase.pool).claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    })).resolves.toEqual({
      status: "in-progress",
      firstSeenAt: firstSeenAt.toISOString()
    });
    expect(contestedDatabase.calls[2]?.sql).toContain("AND status = 'processing'");
    expect(contestedDatabase.calls[2]?.sql).toContain("diagnostic_metadata->>'idempotencyClaimToken' = $4");

    const malformedDatabase = scriptedPool([
      result(0),
      result(1, [
        {
          status: "processing",
          received_at: firstSeenAt,
          processed_at: null,
          claim_token: null,
          lease_acquired_at_epoch_seconds: null
        }
      ])
    ]);
    await expect(new PostgresApprovalStateStore(malformedDatabase.pool).claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    })).resolves.toEqual({
      status: "in-progress",
      firstSeenAt: firstSeenAt.toISOString()
    });
    expect(malformedDatabase.calls).toHaveLength(2);

    const completedAt = new Date("2026-08-01T11:59:30.000Z");
    const completedDatabase = scriptedPool([
      result(0),
      result(1, [
        {
          status: "processed",
          received_at: firstSeenAt,
          processed_at: completedAt,
          claim_token: null,
          lease_acquired_at_epoch_seconds: null
        }
      ])
    ]);
    await expect(new PostgresApprovalStateStore(completedDatabase.pool).claim(envelope.idempotencyKey, {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    })).resolves.toEqual({
      status: "already-completed",
      firstSeenAt: firstSeenAt.toISOString(),
      completedAt: completedAt.toISOString()
    });
    expect(completedDatabase.calls).toHaveLength(2);
  });

  it("uses claim-token compare-and-set for PostgreSQL completion and failure", async () => {
    const completedDatabase = scriptedPool([
      result(1, [
        {
          idempotency_key: "approval-key"
        }
      ])
    ]);
    const completedStore = new PostgresApprovalStateStore(completedDatabase.pool);

    await expect(completedStore.markCompleted("approval-key", completion("completion-token"))).resolves.toBeUndefined();
    expect(completedDatabase.calls[0]?.sql).toContain("AND status = 'processing'");
    expect(completedDatabase.calls[0]?.sql).toContain("diagnostic_metadata->>'idempotencyClaimToken' = $4");
    expect(completedDatabase.calls[0]?.sql).toContain("diagnostic_metadata - 'idempotencyClaimToken'");
    expect(completedDatabase.calls[0]?.sql).toContain("- 'idempotencyLeaseAcquiredAtEpochSeconds'");
    expect(completedDatabase.calls[0]?.values?.[3]).toBe("completion-token");

    const failedDatabase = scriptedPool([
      result(1, [
        {
          idempotency_key: "approval-key"
        }
      ])
    ]);
    const failedStore = new PostgresApprovalStateStore(failedDatabase.pool);

    await expect(failedStore.markFailed("approval-key", failure("failure-token"))).resolves.toBeUndefined();
    expect(failedDatabase.calls[0]?.sql).toContain("AND status = 'processing'");
    expect(failedDatabase.calls[0]?.sql).toContain("diagnostic_metadata->>'idempotencyClaimToken' = $5");
    expect(failedDatabase.calls[0]?.values?.[4]).toBe("failure-token");

    await expect(new PostgresApprovalStateStore(scriptedPool([
      result(0)
    ]).pool).markCompleted("approval-key", completion("wrong-token"))).rejects.toThrow("owned by another delivery");
    await expect(new PostgresApprovalStateStore(scriptedPool([
      result(0)
    ]).pool).markFailed("approval-key", failure("wrong-token"))).rejects.toThrow("owned by another delivery");
  });

  it("distinguishes released, preserved-completed, and not-owned PostgreSQL claims", async () => {
    const releasedDatabase = scriptedPool([
      result(1, [
        {
          idempotency_key: "approval-key"
        }
      ])
    ]);
    await expect(new PostgresApprovalStateStore(releasedDatabase.pool).releaseClaim(
      "approval-key",
      failure("owned-token")
    )).resolves.toEqual({
      status: "released"
    });
    expect(releasedDatabase.calls[0]?.sql).toContain("diagnostic_metadata->>'idempotencyClaimToken' = $5");
    expect(releasedDatabase.calls[0]?.values?.[4]).toBe("owned-token");

    const completedDatabase = scriptedPool([
      result(0),
      result(1, [
        {
          status: "processed"
        }
      ])
    ]);
    await expect(new PostgresApprovalStateStore(completedDatabase.pool).releaseClaim(
      "approval-key",
      failure("stale-token")
    )).resolves.toEqual({
      status: "preserved-completed"
    });

    const notOwnedDatabase = scriptedPool([
      result(0),
      result(1, [
        {
          status: "processing"
        }
      ])
    ]);
    await expect(new PostgresApprovalStateStore(notOwnedDatabase.pool).releaseClaim(
      "approval-key",
      failure("stale-token")
    )).resolves.toEqual({
      status: "not-owned"
    });
  });
});

function completion(claimToken: string) {
  return {
    completedAt: "2026-08-01T12:00:01.000Z",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4801",
    claimToken,
    stage: "approval"
  } as const;
}

function failure(claimToken: string) {
  return {
    failedAt: "2026-08-01T12:00:01.000Z",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b4801",
    claimToken,
    stage: "approval",
    reason: "idempotency-completion-error",
    retryable: true
  } as const;
}

interface ScriptedResult {
  readonly rowCount: number;
  readonly rows: readonly Record<string, unknown>[];
}

interface QueryCall {
  readonly sql: string;
  readonly values: readonly unknown[] | undefined;
}

function result(rowCount: number, rows: readonly Record<string, unknown>[] = []): ScriptedResult {
  return {
    rowCount,
    rows
  };
}

function scriptedPool(responses: readonly ScriptedResult[]): {
  readonly pool: Pool;
  readonly calls: QueryCall[];
} {
  const pending = [
    ...responses
  ];
  const calls: QueryCall[] = [];
  const query = vi.fn((sql: string, values?: readonly unknown[]) => {
    calls.push({
      sql,
      values
    });
    const response = pending.shift();

    if (response === undefined) {
      return Promise.reject(new Error("Unexpected PostgreSQL query."));
    }

    return Promise.resolve(response);
  });

  return {
    pool: {
      query
    } as unknown as Pool,
    calls
  };
}

function jsonArgument(call: QueryCall | undefined, index: number): Readonly<Record<string, unknown>> {
  const value = call?.values?.[index];

  if (typeof value !== "string") {
    throw new Error("Expected a JSON query argument.");
  }

  return JSON.parse(value) as Readonly<Record<string, unknown>>;
}
