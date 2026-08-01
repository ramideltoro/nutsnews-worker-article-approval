import type { Pool } from "pg";
import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

import {
  APPROVAL_IDEMPOTENCY_LEASE_SECONDS,
  APPROVAL_IDEMPOTENCY_RENEWAL_INTERVAL_MS,
  PostgresApprovalStateStore
} from "../src/production.js";
import { ApprovalClaimOwnershipError } from "../src/dependencies.js";
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
    expect(database.calls[0]?.sql).toContain("'claimedAt', clock_timestamp()");
    expect(database.calls[0]?.sql).toContain("'idempotencyLeaseAcquiredAtEpochSeconds', extract(epoch FROM clock_timestamp())");
    expect(APPROVAL_IDEMPOTENCY_LEASE_SECONDS).toBeGreaterThan(0);
    expect(APPROVAL_IDEMPOTENCY_LEASE_SECONDS).toBeLessThanOrEqual(5 * 60);
    await store.close();
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
    expect(database.calls[2]?.sql).toContain("'idempotencyLeaseAcquiredAtEpochSeconds', extract(epoch FROM clock_timestamp())");
    await store.close();
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
    expect(reclaim?.sql).toContain("extract(epoch FROM clock_timestamp()) - $6::numeric");
    expect(reclaim?.values?.[2]).toBe(reclaimed.claimToken);
    expect(reclaim?.values?.[3]).toBe("prior-claim-token");
    expect(reclaim?.values?.[4]).toBe("1785585000.125");
    expect(reclaim?.values?.[5]).toBe(APPROVAL_IDEMPOTENCY_LEASE_SECONDS);
    await store.close();
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

  it("uses claim-token and unexpired-lease compare-and-set for PostgreSQL completion and failure", async () => {
    const completedDatabase = scriptedPool([
      result(1, [
        {
          received_at: new Date(RECEIVED_AT)
        }
      ]),
      result(1, [
        {
          idempotency_key: "approval-key"
        }
      ])
    ]);
    const completedStore = new PostgresApprovalStateStore(completedDatabase.pool);
    const claimed = await claimPostgres(completedStore, "approval-key");

    await expect(completedStore.markCompleted("approval-key", completion(claimed.claimToken))).resolves.toBeUndefined();
    const completionAttempt = completedDatabase.calls[1];
    expect(completionAttempt?.sql).toContain("AND status = 'processing'");
    expect(completionAttempt?.sql).toContain("diagnostic_metadata->>'idempotencyClaimToken' = $4");
    expect(completionAttempt?.sql).toContain("diagnostic_metadata - 'idempotencyClaimToken'");
    expect(completionAttempt?.sql).toContain("- 'idempotencyLeaseAcquiredAtEpochSeconds'");
    expect(completionAttempt?.sql).toContain("jsonb_typeof(diagnostic_metadata->'idempotencyLeaseAcquiredAtEpochSeconds') = 'number'");
    expect(completionAttempt?.sql).toContain("> extract(epoch FROM clock_timestamp()) - $5::numeric");
    expect(completionAttempt?.values?.[3]).toBe(claimed.claimToken);
    expect(completionAttempt?.values?.[4]).toBe(APPROVAL_IDEMPOTENCY_LEASE_SECONDS);

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
    expect(failedDatabase.calls[0]?.sql).toContain("> extract(epoch FROM clock_timestamp()) - $6::numeric");
    expect(failedDatabase.calls[0]?.values?.[4]).toBe("failure-token");
    expect(failedDatabase.calls[0]?.values?.[5]).toBe(APPROVAL_IDEMPOTENCY_LEASE_SECONDS);

    await expect(new PostgresApprovalStateStore(scriptedPool([
      result(0)
    ]).pool).markCompleted("approval-key", completion("wrong-token"))).rejects.toThrow("matching active ownership");
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
    expect(releasedDatabase.calls[0]?.sql).toContain("> extract(epoch FROM clock_timestamp()) - $6::numeric");
    expect(releasedDatabase.calls[0]?.values?.[4]).toBe("owned-token");
    expect(releasedDatabase.calls[0]?.values?.[5]).toBe(APPROVAL_IDEMPOTENCY_LEASE_SECONDS);

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

  it("expires owner mutation rights before reclaim and gives the exact boundary to the reclaimer", async () => {
    const firstSeenAt = new Date("2026-08-01T11:50:00.000Z");
    const database = scriptedPool([
      result(1, [
        {
          received_at: firstSeenAt
        }
      ]),
      result(0),
      result(0),
      result(1, [
        {
          status: "processing"
        }
      ]),
      result(0),
      result(1, [
        {
          status: "processing",
          received_at: firstSeenAt,
          processed_at: null,
          claim_token: "owned-token-placeholder",
          lease_acquired_at_epoch_seconds: "1785585000.000"
        }
      ]),
      result(1, [
        {
          received_at: firstSeenAt
        }
      ])
    ]);
    const store = new PostgresApprovalStateStore(database.pool);
    const owned = await claimPostgres(store, "approval-key");

    await expect(store.markCompleted("approval-key", completion(owned.claimToken))).rejects.toThrow(
      "owned by another delivery"
    );
    await expect(store.releaseClaim("approval-key", failure(owned.claimToken))).resolves.toEqual({
      status: "not-owned"
    });

    const envelope = createMinimalApprovalEnvelope({
      idempotencyKey: "approval-key"
    });
    const reclaimed = await store.claim("approval-key", {
      envelope,
      stage: "approval",
      receivedAt: RECEIVED_AT
    });
    expect(reclaimed).toMatchObject({
      status: "claimed",
      replay: true
    });
    if (reclaimed.status !== "claimed") {
      throw new Error("Expected the expired lease to transfer to a new owner.");
    }
    expect(reclaimed.claimToken).not.toBe(owned.claimToken);

    const completionAttempt = database.calls[1];
    const releaseAttempt = database.calls[2];
    const reclaimAttempt = database.calls[6];
    expect(completionAttempt?.sql).toContain("> extract(epoch FROM clock_timestamp()) - $5::numeric");
    expect(completionAttempt?.values?.[4]).toBe(APPROVAL_IDEMPOTENCY_LEASE_SECONDS);
    expect(releaseAttempt?.sql).toContain("> extract(epoch FROM clock_timestamp()) - $6::numeric");
    expect(releaseAttempt?.values?.[5]).toBe(APPROVAL_IDEMPOTENCY_LEASE_SECONDS);
    expect(reclaimAttempt?.sql).toContain("<= extract(epoch FROM clock_timestamp()) - $6::numeric");
    expect(reclaimAttempt?.values?.[5]).toBe(APPROVAL_IDEMPOTENCY_LEASE_SECONDS);
    await store.close();
  });

  describe("PostgreSQL claim heartbeat", () => {
    afterEach(() => {
      vi.useRealTimers();
    });

    it("renews an owned lease from the database clock and stops after completion", async () => {
      vi.useFakeTimers();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ]),
        result(1, [
          {
            idempotency_key: "approval-key"
          }
        ]),
        result(1, [
          {
            idempotency_key: "approval-key"
          }
        ])
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 500
      });
      const claimed = await claimPostgres(store, "approval-key");
      const ownership = store.ownership("approval-key");

      expect(ownership.signal.aborted).toBe(false);
      expect(vi.getTimerCount()).toBe(1);
      await vi.advanceTimersByTimeAsync(1_000);

      const renewal = findRenewalCall(database.calls);
      expect(renewal?.sql).toContain("AND status = 'processing'");
      expect(renewal?.sql).toContain("diagnostic_metadata->>'idempotencyClaimToken' = $2");
      expect(renewal?.sql).toContain("'claimedAt', clock_timestamp()");
      expect(renewal?.sql).toContain("'idempotencyLeaseAcquiredAtEpochSeconds', extract(epoch FROM clock_timestamp())");
      expect(renewal?.sql).toContain("'leaseRenewedAt', clock_timestamp()");
      expect(renewal?.sql).toContain("> extract(epoch FROM clock_timestamp()) - $3::numeric");
      expect(renewal?.values).toEqual([
        "approval-key",
        claimed.claimToken,
        APPROVAL_IDEMPOTENCY_LEASE_SECONDS
      ]);
      expect(vi.getTimerCount()).toBe(1);

      await expect(store.markCompleted("approval-key", completion(claimed.claimToken))).resolves.toBeUndefined();
      expect(vi.getTimerCount()).toBe(0);
      expect(() => store.ownership("approval-key")).toThrow(ApprovalClaimOwnershipError);
    });

    it("does not stop the current heartbeat for a stale terminal token", async () => {
      vi.useFakeTimers();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ]),
        result(1, [
          {
            idempotency_key: "approval-key"
          }
        ]),
        result(1, [
          {
            idempotency_key: "approval-key"
          }
        ])
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 500
      });
      const claimed = await claimPostgres(store, "approval-key");

      await expect(store.markCompleted("approval-key", completion("stale-token"))).rejects.toThrow(
        "matching active ownership"
      );
      expect(store.ownership("approval-key").signal.aborted).toBe(false);
      expect(vi.getTimerCount()).toBe(1);

      await vi.advanceTimersByTimeAsync(1_000);
      expect(findRenewalCall(database.calls)?.values).toEqual([
        "approval-key",
        claimed.claimToken,
        APPROVAL_IDEMPOTENCY_LEASE_SECONDS
      ]);
      expect(store.ownership("approval-key").signal.aborted).toBe(false);

      await expect(store.releaseClaim("approval-key", failure(claimed.claimToken))).resolves.toEqual({
        status: "released"
      });
      expect(vi.getTimerCount()).toBe(0);
    });

    it("fails closed when renewal loses the exact unexpired boundary", async () => {
      vi.useFakeTimers();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ]),
        result(0)
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 500
      });
      await claimPostgres(store, "approval-key");
      const ownership = store.ownership("approval-key");

      await vi.advanceTimersByTimeAsync(1_000);

      expect(findRenewalCall(database.calls)?.sql).toContain("> extract(epoch FROM clock_timestamp()) - $3::numeric");
      expect(ownership.signal.aborted).toBe(true);
      expect(ownership.signal.reason).toBeInstanceOf(ApprovalClaimOwnershipError);
      expect(() => ownership.assertOwned()).toThrow("lost ownership");
      expect(vi.getTimerCount()).toBe(0);
      await store.close();
    });

    it("keeps renewal single-flight even when several intervals elapse", async () => {
      vi.useFakeTimers();
      const renewal = deferred<ScriptedResult>();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ]),
        renewal.promise,
        result(1, [
          {
            idempotency_key: "approval-key"
          }
        ])
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 30_000
      });
      await claimPostgres(store, "approval-key");

      vi.advanceTimersByTime(1_000);
      await flushMicrotasks();
      expect(findRenewalCalls(database.calls)).toHaveLength(1);

      vi.advanceTimersByTime(10_000);
      await flushMicrotasks();
      expect(findRenewalCalls(database.calls)).toHaveLength(1);
      expect(vi.getTimerCount()).toBe(1);

      renewal.resolve(result(1, [
        {
          idempotency_key: "approval-key"
        }
      ]));
      await vi.advanceTimersByTimeAsync(0);
      expect(vi.getTimerCount()).toBe(1);

      await vi.advanceTimersByTimeAsync(1_000);
      expect(findRenewalCalls(database.calls)).toHaveLength(2);
      await store.close();
      expect(vi.getTimerCount()).toBe(0);
    });

    it.each([
      {
        name: "completion",
        response: result(1, [
          {
            idempotency_key: "approval-key"
          }
        ]),
        finish: (store: PostgresApprovalStateStore, token: string) => store.markCompleted(
          "approval-key",
          completion(token)
        )
      },
      {
        name: "failure",
        response: result(1, [
          {
            idempotency_key: "approval-key"
          }
        ]),
        finish: (store: PostgresApprovalStateStore, token: string) => store.markFailed(
          "approval-key",
          failure(token)
        )
      },
      {
        name: "release",
        response: result(1, [
          {
            idempotency_key: "approval-key"
          }
        ]),
        finish: (store: PostgresApprovalStateStore, token: string) => store.releaseClaim(
          "approval-key",
          failure(token)
        )
      }
    ])("cleans up timers after $name", async ({ response, finish }) => {
      vi.useFakeTimers();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ]),
        response
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 500
      });
      const claimed = await claimPostgres(store, "approval-key");

      expect(vi.getTimerCount()).toBe(1);
      await finish(store, claimed.claimToken);

      expect(vi.getTimerCount()).toBe(0);
      expect(() => store.ownership("approval-key")).toThrow(ApprovalClaimOwnershipError);
    });

    it("aborts ownership and clears timers when the store closes", async () => {
      vi.useFakeTimers();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ])
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 500
      });
      const claimed = await claimPostgres(store, "approval-key");
      const ownership = store.ownership("approval-key");

      await store.close();

      expect(ownership.signal.aborted).toBe(true);
      expect(ownership.signal.reason).toBeInstanceOf(ApprovalClaimOwnershipError);
      expect(vi.getTimerCount()).toBe(0);
      expect(() => store.ownership("approval-key")).toThrow(ApprovalClaimOwnershipError);
      await expect(store.markCompleted("approval-key", completion(claimed.claimToken))).rejects.toThrow(
        "matching active ownership"
      );
      expect(database.calls).toHaveLength(1);
    });

    it("waits for an in-flight acquisition and releases it when close wins the race", async () => {
      vi.useFakeTimers();
      const insert = deferred<ScriptedResult>();
      const database = scriptedPool([
        insert.promise,
        result(1, [
          {
            idempotency_key: "approval-key"
          }
        ])
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 500
      });
      const claiming = claimPostgres(store, "approval-key");
      let closeFinished = false;
      const closing = store.close().then(() => {
        closeFinished = true;
      });
      await flushMicrotasks();

      expect(closeFinished).toBe(false);
      insert.resolve(result(1, [
        {
          received_at: new Date(RECEIVED_AT)
        }
      ]));

      await expect(claiming).rejects.toThrow("stopped accepting claims during acquisition");
      await expect(closing).resolves.toBeUndefined();
      expect(closeFinished).toBe(true);
      expect(database.calls[1]?.sql).toContain("SET status = 'failed'");
      expect(database.calls[1]?.values?.[4]).toMatch(/^[0-9a-f-]{36}$/u);
      expect(vi.getTimerCount()).toBe(0);
      expect(() => store.ownership("approval-key")).toThrow(ApprovalClaimOwnershipError);
    });

    it("fails closed when a renewal query fails", async () => {
      vi.useFakeTimers();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ]),
        new Error("renewal database unavailable")
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 500
      });
      await claimPostgres(store, "approval-key");
      const ownership = store.ownership("approval-key");

      await vi.advanceTimersByTimeAsync(1_000);

      expect(ownership.signal.aborted).toBe(true);
      expect(ownership.signal.reason).toBeInstanceOf(Error);
      expect((ownership.signal.reason as Error).message).toContain("renewal database unavailable");
      expect(vi.getTimerCount()).toBe(0);
      await store.close();
    });

    it("destroys the renewal connection and fails closed on query timeout", async () => {
      vi.useFakeTimers();
      const pendingRenewal = abortableResponse();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ]),
        pendingRenewal.response
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 500
      });
      await claimPostgres(store, "approval-key");
      const ownership = store.ownership("approval-key");

      await vi.advanceTimersByTimeAsync(1_500);

      expect(ownership.signal.aborted).toBe(true);
      expect((ownership.signal.reason as Error).message).toContain("renewal timed out");
      expect(pendingRenewal.aborted).toBe(true);
      expect(database.releases.some((reason) => reason instanceof Error)).toBe(true);
      expect(vi.getTimerCount()).toBe(0);
      await store.close();
    });

    it("waits for an in-flight renewal to be cancelled before close resolves", async () => {
      vi.useFakeTimers();
      const pendingRenewal = abortableResponse();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ]),
        pendingRenewal.response
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 30_000
      });
      await claimPostgres(store, "approval-key");
      const ownership = store.ownership("approval-key");
      vi.advanceTimersByTime(1_000);
      await flushMicrotasks();

      const closing = store.close();
      await expect(closing).resolves.toBeUndefined();

      expect(pendingRenewal.aborted).toBe(true);
      expect(ownership.signal.aborted).toBe(true);
      expect(database.releases.some((reason) => reason instanceof Error)).toBe(true);
      expect(vi.getTimerCount()).toBe(0);
    });

    it("rejects completion when a concurrent renewal loses ownership", async () => {
      vi.useFakeTimers();
      const renewal = deferred<ScriptedResult>();
      const database = scriptedPool([
        result(1, [
          {
            received_at: new Date(RECEIVED_AT)
          }
        ]),
        renewal.promise
      ]);
      const store = new PostgresApprovalStateStore(database.pool, {
        renewalIntervalMs: 1_000,
        renewalQueryTimeoutMs: 30_000
      });
      const claimed = await claimPostgres(store, "approval-key");
      vi.advanceTimersByTime(1_000);
      await flushMicrotasks();

      const completing = store.markCompleted("approval-key", completion(claimed.claimToken));
      renewal.resolve(result(0));

      await expect(completing).rejects.toThrow("lost ownership");
      expect(database.calls.some((call) => call.sql.includes("SET status = 'processed'"))).toBe(false);
      expect(vi.getTimerCount()).toBe(0);
    });

    it("rejects a renewal interval above the sixty-second safety bound", () => {
      expect(() => new PostgresApprovalStateStore(scriptedPool([]).pool, {
        renewalIntervalMs: APPROVAL_IDEMPOTENCY_RENEWAL_INTERVAL_MS + 1
      })).toThrow("renewalIntervalMs");
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

interface AbortableResponse {
  readonly kind: "abortable";
  readonly promise: Promise<ScriptedResult>;
  abort(reason: Error): void;
}

type ScriptedResponse = ScriptedResult | Promise<ScriptedResult> | Error | AbortableResponse;

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

function scriptedPool(responses: readonly ScriptedResponse[]): {
  readonly pool: Pool;
  readonly calls: QueryCall[];
  readonly releases: readonly (Error | boolean | undefined)[];
} {
  const pending = [
    ...responses
  ];
  const calls: QueryCall[] = [];
  const releases: (Error | boolean | undefined)[] = [];
  const activeAbortables = new Set<AbortableResponse>();
  const query = vi.fn((sql: string, values?: readonly unknown[]) => {
    calls.push({
      sql,
      values
    });

    if (sql.startsWith("SET statement_timeout") || sql === "RESET statement_timeout") {
      return Promise.resolve(result(0));
    }

    const response = pending.shift();

    if (response === undefined) {
      return Promise.reject(new Error("Unexpected PostgreSQL query."));
    }

    if (response instanceof Error) {
      return Promise.reject(response);
    }

    if (isAbortableResponse(response)) {
      activeAbortables.add(response);
      return response.promise.finally(() => {
        activeAbortables.delete(response);
      });
    }

    return Promise.resolve(response);
  });
  const release = vi.fn((reason?: Error | boolean) => {
    releases.push(reason);

    if (reason !== undefined) {
      const error = reason instanceof Error
        ? reason
        : new Error("PostgreSQL client was destroyed.");

      for (const response of activeAbortables) {
        response.abort(error);
      }
    }
  });
  const connect = vi.fn(() => Promise.resolve({
    query,
    release
  }));

  return {
    pool: {
      query,
      connect
    } as unknown as Pool,
    calls,
    releases
  };
}

function abortableResponse(): {
  readonly response: AbortableResponse;
  readonly aborted: boolean;
} {
  const pending = deferred<ScriptedResult>();
  let aborted = false;

  return {
    response: {
      kind: "abortable",
      promise: pending.promise,
      abort: (reason) => {
        aborted = true;
        pending.reject(reason);
      }
    },
    get aborted(): boolean {
      return aborted;
    }
  };
}

function isAbortableResponse(response: ScriptedResponse): response is AbortableResponse {
  return "kind" in response;
}

function findRenewalCall(calls: readonly QueryCall[]): QueryCall | undefined {
  return calls.find((call) => call.sql.includes("'leaseRenewedAt'"));
}

function findRenewalCalls(calls: readonly QueryCall[]): readonly QueryCall[] {
  return calls.filter((call) => call.sql.includes("'leaseRenewedAt'"));
}

async function claimPostgres(
  store: PostgresApprovalStateStore,
  idempotencyKey: string
): Promise<Extract<Awaited<ReturnType<PostgresApprovalStateStore["claim"]>>, { readonly status: "claimed" }>> {
  const envelope = createMinimalApprovalEnvelope({
    idempotencyKey
  });
  const claimed = await store.claim(idempotencyKey, {
    envelope,
    stage: "approval",
    receivedAt: RECEIVED_AT
  });

  if (claimed.status !== "claimed") {
    throw new Error("Expected a claimed PostgreSQL idempotency lease.");
  }

  return claimed;
}

function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject
  };
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

function jsonArgument(call: QueryCall | undefined, index: number): Readonly<Record<string, unknown>> {
  const value = call?.values?.[index];

  if (typeof value !== "string") {
    throw new Error("Expected a JSON query argument.");
  }

  return JSON.parse(value) as Readonly<Record<string, unknown>>;
}
