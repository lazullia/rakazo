import { createHash, randomUUID } from "node:crypto";
import type { Pool } from "pg";
import type { PiStage, PiStageRecord, PiStageStore } from "./pi-cloud-agent.js";

/** Minimal SQL boundary; accepts a normal pg Pool without reading any environment URL. */
export interface PiStageSqlConnection {
  query(sql: string, values?: unknown[]): Promise<{ rows: Array<Record<string, unknown>> }>;
  release(error?: Error): void;
}
export interface PiStageSqlPool {
  connect(): Promise<PiStageSqlConnection>;
}

type Scope = { userId: string; spaceId: string };
type Stages = Partial<Record<PiStage, PiStageRecord>>;
type WorkRow = { id: string; workId: string; stages: Stages; stage?: PiStage };

export class PiStageStateError extends Error {
  constructor() {
    super("Pi stage binding or state is invalid.");
  }
}
function fail(): never {
  throw new PiStageStateError();
}

function immutable(record: PiStageRecord) {
  return JSON.stringify({
    handle: record.handle,
    runnerNamespace: record.runnerNamespace,
    workId: record.workId,
    stage: record.stage,
    ownerId: record.ownerId,
    spaceId: record.spaceId,
    botId: record.botId,
    threadId: record.threadId,
    originRunId: record.originRunId,
    approvalRef: record.approvalRef,
    startUntil: record.startUntil,
    workUntil: record.workUntil,
    stopReconcileUntil: record.stopReconcileUntil,
    input: record.input,
    targetHash: record.targetHash,
    returnTo: record.returnTo,
  });
}

function validate(
  record: PiStageRecord | undefined,
  stage: PiStage,
  scope: Scope,
  workId: string,
): asserts record is PiStageRecord {
  if (
    !record ||
    record.stage !== stage ||
    record.workId !== workId ||
    record.ownerId !== scope.userId ||
    record.spaceId !== scope.spaceId
  )
    fail();
  for (const field of [
    record.handle,
    record.runnerNamespace,
    record.workId,
    record.botId,
    record.threadId,
    record.originRunId,
    record.approvalRef,
    record.returnTo?.botId,
    record.returnTo?.threadId,
  ]) {
    if (typeof field !== "string" || !field.trim()) fail();
  }
  if (
    typeof record.input !== "string" ||
    !["reserved", "running", "completed", "unknown", "cancelled"].includes(record.state)
  )
    fail();
  for (const field of [
    record.approvalValid,
    record.sharingAllowed,
    record.dispatched,
    record.cancelRequested,
    record.stopConfirmed,
  ])
    if (typeof field !== "boolean") fail();
  for (const field of [record.startUntil, record.workUntil, record.stopReconcileUntil])
    if (!Number.isFinite(field)) fail();
  if (!Number.isSafeInteger(record.dispatchGeneration) || record.dispatchGeneration < 0) fail();
  if (
    record.dispatchClaim &&
    (typeof record.dispatchClaim.owner !== "string" ||
      !record.dispatchClaim.owner ||
      record.dispatchClaim.generation !== record.dispatchGeneration ||
      !Number.isFinite(record.dispatchClaim.until))
  )
    fail();
  if (
    (record.dispatched && record.dispatchGeneration === 0) ||
    (record.state === "running" && !record.dispatched) ||
    (record.state === "completed" && (!record.original || !record.stopConfirmed)) ||
    (record.state === "cancelled" && !record.stopConfirmed) ||
    (record.dispatchClaim &&
      (!record.dispatched || record.dispatchClaim.until > record.workUntil)) ||
    (record.reconciliationExpired !== undefined &&
      typeof record.reconciliationExpired !== "boolean")
  )
    fail();
  if (
    record.original &&
    (typeof record.original.text !== "string" ||
      createHash("sha256").update(record.original.text).digest("hex") !== record.original.hash ||
      record.original.targetHash !== record.targetHash)
  )
    fail();
}

const transitions: Record<PiStageRecord["state"], readonly PiStageRecord["state"][]> = {
  reserved: ["reserved", "running", "completed", "unknown", "cancelled"],
  running: ["running", "completed", "unknown", "cancelled"],
  completed: ["completed", "unknown", "cancelled"],
  unknown: ["unknown", "cancelled"],
  cancelled: ["cancelled"],
};

function checkTransition(before: PiStageRecord, after: PiStageRecord) {
  if (
    immutable(before) !== immutable(after) ||
    (before.dispatched && !after.dispatched) ||
    (before.stopConfirmed && !after.stopConfirmed) ||
    (before.cancelRequested && !after.cancelRequested) ||
    (!before.approvalValid && after.approvalValid) ||
    (!before.sharingAllowed && after.sharingAllowed) ||
    after.dispatchGeneration < before.dispatchGeneration ||
    !transitions[before.state].includes(after.state) ||
    (before.reconciliationExpired === true && after.reconciliationExpired !== true) ||
    (before.state === "cancelled" && before.stopConfirmed && after.state !== "cancelled") ||
    (before.original && JSON.stringify(before.original) !== JSON.stringify(after.original))
  )
    fail();
}

/**
 * Server-injected tenant scope only. bindTrustedStage is internal persistence, NOT
 * an approval API: the owner-consent/normal-tool gate must run before registration.
 * One work row lock serializes both stages across pools/processes, never globally.
 */
export class PostgresPiStageStore implements PiStageStore {
  constructor(
    private readonly pool: PiStageSqlPool,
    private readonly scope: Scope,
  ) {
    if (!scope.userId.trim() || !scope.spaceId.trim()) fail();
  }

  private async transaction<T>(run: (client: PiStageSqlConnection) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    let releaseError: Error | undefined;
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT set_config('app.user_id', $1, true), set_config('app.space_id', $2, true), set_config('app.rls_bypass', 'off', true)",
        [this.scope.userId, this.scope.spaceId],
      );
      const value = await run(client);
      await client.query("COMMIT");
      return value;
    } catch {
      await client.query("ROLLBACK").catch(() => {
        releaseError = new PiStageStateError();
      });
      // No SQL error detail, input or original result is echoed to a caller.
      throw new PiStageStateError();
    } finally {
      client.release(releaseError);
    }
  }

  async bindTrustedStage(agentId: string, record: PiStageRecord): Promise<void> {
    if (!agentId.trim() || !["design", "review"].includes(record.stage)) fail();
    validate(record, record.stage, this.scope, record.workId);
    if (
      record.dispatched ||
      record.dispatchGeneration !== 0 ||
      record.dispatchClaim ||
      record.original ||
      record.state !== "reserved" ||
      record.cancelRequested ||
      record.stopConfirmed ||
      !record.approvalValid ||
      !record.sharingAllowed
    )
      fail();
    await this.transaction(async (client) => {
      await client.query(
        'INSERT INTO pi_work_states (id, "ownerId", "spaceId", "workId") VALUES ($1,$2,$3,$4) ON CONFLICT ("ownerId", "spaceId", "workId") DO NOTHING',
        [randomUUID(), this.scope.userId, this.scope.spaceId, record.workId],
      );
      const found = await client.query(
        'SELECT id, "workId", stages FROM pi_work_states WHERE "ownerId"=$1 AND "spaceId"=$2 AND "workId"=$3 FOR UPDATE',
        [this.scope.userId, this.scope.spaceId, record.workId],
      );
      const row = found.rows[0] as WorkRow | undefined;
      if (!row) fail();
      await client.query(
        'INSERT INTO pi_stage_handles ("workStateId", "ownerId", "spaceId", stage, "runnerNamespace", handle) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT ("workStateId", stage) DO NOTHING',
        [
          row.id,
          this.scope.userId,
          this.scope.spaceId,
          record.stage,
          record.runnerNamespace,
          record.handle,
        ],
      );
      const canonical = await client.query(
        'SELECT "runnerNamespace", handle FROM pi_stage_handles WHERE "workStateId"=$1 AND stage=$2',
        [row.id, record.stage],
      );
      if (
        canonical.rows[0]?.runnerNamespace !== record.runnerNamespace ||
        canonical.rows[0]?.handle !== record.handle
      )
        fail();
      const previous = row.stages[record.stage];
      if (previous) {
        validate(previous, record.stage, this.scope, row.workId);
        if (immutable(previous) !== immutable(record)) fail();
      } else {
        row.stages[record.stage] = structuredClone(record);
        await client.query(
          'UPDATE pi_work_states SET stages=$2::jsonb, "updatedAt"=CURRENT_TIMESTAMP WHERE id=$1',
          [row.id, JSON.stringify(row.stages)],
        );
      }
      await client.query(
        'INSERT INTO pi_stage_bindings ("agentId", "workStateId", "ownerId", "spaceId", stage) VALUES ($1,$2,$3,$4,$5) ON CONFLICT ("agentId") DO NOTHING',
        [agentId, row.id, this.scope.userId, this.scope.spaceId, record.stage],
      );
      const alias = await client.query(
        'SELECT "workStateId", stage FROM pi_stage_bindings WHERE "agentId"=$1',
        [agentId],
      );
      if (alias.rows[0]?.workStateId !== row.id || alias.rows[0]?.stage !== record.stage) fail();
    });
  }

  private async lookup(client: PiStageSqlConnection, agentId: string, lock: boolean) {
    const found = await client.query(
      `SELECT w.id, w."workId", w.stages, b.stage FROM pi_work_states w JOIN pi_stage_bindings b ON b."workStateId"=w.id WHERE b."agentId"=$1${lock ? " FOR UPDATE OF w" : ""}`,
      [agentId],
    );
    const row = found.rows[0] as WorkRow | undefined;
    if (!row) return undefined;
    if (row.stage !== "design" && row.stage !== "review") fail();
    validate(row.stages[row.stage], row.stage, this.scope, row.workId);
    for (const stage of ["design", "review"] as const)
      if (row.stages[stage]) validate(row.stages[stage], stage, this.scope, row.workId);
    const canonical = await client.query(
      'SELECT stage, "runnerNamespace", handle FROM pi_stage_handles WHERE "workStateId"=$1',
      [row.id],
    );
    for (const stage of ["design", "review"] as const) {
      const record = row.stages[stage];
      if (!record) continue;
      const handle = canonical.rows.find((item) => item.stage === stage);
      if (handle?.runnerNamespace !== record.runnerNamespace || handle?.handle !== record.handle)
        fail();
    }
    return row;
  }

  async read(agentId: string): Promise<PiStageRecord | undefined> {
    return this.transaction(async (client) => {
      const row = await this.lookup(client, agentId, false);
      return row ? structuredClone(row.stages[row.stage!]) : undefined;
    });
  }

  async change<T>(
    agentId: string,
    update: (record: PiStageRecord, design?: PiStageRecord) => T,
  ): Promise<T> {
    return this.transaction(async (client) => {
      const row = await this.lookup(client, agentId, true);
      if (!row) fail();
      const before = structuredClone(row.stages);
      const value = update(row.stages[row.stage!]!, row.stages.design);
      if (value && typeof (value as { then?: unknown }).then === "function") fail();
      for (const stage of ["design", "review"] as const) {
        if (before[stage]) {
          validate(row.stages[stage], stage, this.scope, row.workId);
          checkTransition(before[stage], row.stages[stage]!);
        } else if (row.stages[stage]) fail();
      }
      if (JSON.stringify(before) !== JSON.stringify(row.stages)) {
        await client.query(
          'UPDATE pi_work_states SET stages=$2::jsonb, "updatedAt"=CURRENT_TIMESTAMP WHERE id=$1',
          [row.id, JSON.stringify(row.stages)],
        );
      }
      return value;
    });
  }
}

/** Structural check against the real pg connection API; no pool is instantiated. */
export function postgresPiStageStore(pool: Pool, scope: Scope) {
  return new PostgresPiStageStore(pool, scope);
}
