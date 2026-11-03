import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { PiObservation, PiStageRecord, PiStageRunner } from "./pi-cloud-agent.js";
import { PiCloudAgentProvider } from "./pi-cloud-agent.js";
import { binding, context } from "./pi-cloud-agent.test-fixtures.js";
import { PostgresPiStageStore } from "./pi-stage-postgres.js";
import {
  closeTestSqlSessions,
  IsolatedPsqlPool,
  testDatabaseDescriptor,
} from "./pi-stage-postgres.test-db.js";

const enabled = Boolean(process.env.PI_STAGE_TEST_DESCRIPTOR);
const describeDatabase = enabled ? describe.sequential : describe.skip;
const scope = { userId: "owner", spaceId: "space" };

async function sql(text: string, admin = false) {
  const client = await new IsolatedPsqlPool(admin).connect();
  try {
    return await client.query(text);
  } finally {
    client.release();
  }
}
async function scopedSql(text: string, values: unknown[] = [], actor = scope) {
  const client = await new IsolatedPsqlPool().connect();
  try {
    await client.query("BEGIN");
    await client.query(
      "SELECT set_config('app.user_id', $1, true), set_config('app.space_id', $2, true)",
      [actor.userId, actor.spaceId],
    );
    const result = await client.query(text, values);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}
function fresh() {
  const id = randomUUID();
  return {
    agentId: `agent-${id}`,
    record: binding({ workId: `work-${id}`, handle: `handle-${id}` }),
  };
}
async function until(predicate: () => boolean) {
  const deadline = Date.now() + 5000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Fake dispatch did not reach its test barrier");
    await new Promise((done) => setTimeout(done, 10));
  }
}
function store() {
  return new PostgresPiStageStore(new IsolatedPsqlPool(), scope);
}

/** No process/model work. Only fake state transitions, with authority read from real PG. */
class DbFakeRunner implements PiStageRunner {
  readonly namespace = "fake-pi";
  readonly states = new Map<string, PiObservation>();
  starts = 0;
  lookups = 0;
  beforeStart?: () => Promise<void>;
  constructor(
    private readonly records: PostgresPiStageStore,
    private readonly agentId: string,
  ) {}
  async startReserved(record: PiStageRecord): Promise<PiObservation> {
    await this.beforeStart?.();
    const allowed = await this.records.change(
      this.agentId,
      (current, design) =>
        (current.stage !== "review" ||
          (design?.state === "completed" &&
            !design.cancelRequested &&
            design.approvalValid &&
            design.sharingAllowed &&
            design.original?.hash === current.targetHash)) &&
        !current.cancelRequested &&
        current.approvalValid &&
        current.sharingAllowed &&
        current.dispatchClaim?.owner === record.dispatchClaim?.owner &&
        current.dispatchClaim?.generation === record.dispatchClaim?.generation,
    );
    if (!allowed || this.states.get(record.handle)?.kind === "cancelled")
      return { kind: "cancelled", stopConfirmed: true };
    if (!this.states.has(record.handle)) {
      this.starts++;
      this.states.set(record.handle, { kind: "running" });
    }
    return this.states.get(record.handle)!;
  }
  async lookup(handle: string) {
    this.lookups++;
    return this.states.get(handle) ?? { kind: "missing" as const };
  }
  async cancel(handle: string): Promise<PiObservation> {
    this.states.set(handle, { kind: "cancelled", stopConfirmed: true });
    return this.states.get(handle)!;
  }
}

// These tests cannot use DATABASE_URL; the fixture validates a newly owned,
// no-network/no-port/tmpfs-only Docker container before every SQL connection.
describeDatabase("Pi durable work/stage store (dedicated real PostgreSQL only)", () => {
  beforeAll(async () => {
    testDatabaseDescriptor();
    const client = await new IsolatedPsqlPool(true).connect();
    try {
      await client.query(
        "DROP TABLE IF EXISTS pi_stage_bindings; DROP TABLE IF EXISTS pi_stage_handles; DROP TABLE IF EXISTS pi_work_states",
      );
      await client.query(
        readFileSync(
          new URL(
            "../../db/prisma/migrations/20261113090000_pi_stage_state_offline_candidate/migration.sql",
            import.meta.url,
          ),
          "utf8",
        ),
      );
      await client.query(
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='pi_stage_app') THEN CREATE ROLE pi_stage_app LOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$",
      );
      await client.query(
        "REVOKE ALL ON pi_work_states, pi_stage_bindings, pi_stage_handles FROM PUBLIC; GRANT SELECT, INSERT, UPDATE, DELETE ON pi_work_states, pi_stage_bindings, pi_stage_handles TO pi_stage_app",
      );
      const version = await client.query("SELECT version() AS version");
      if (process.env.PI_STAGE_TEST_DB_REPORT)
        writeFileSync(
          process.env.PI_STAGE_TEST_DB_REPORT,
          JSON.stringify(
            {
              version: version.rows[0]?.version,
              migrationApplied: true,
              transport: "psql over docker exec stdin/stdout; no network",
              applicationRole: "NOSUPERUSER NOBYPASSRLS",
            },
            null,
            2,
          ),
        );
    } finally {
      client.release();
    }
  });
  afterAll(closeTestSqlSessions);

  it("persists records and claims across independent store connections, with quoted fake payloads", async () => {
    const f = fresh();
    f.record.input = "Generalized ' quote \\ and $1\nnext line";
    const first = store();
    await first.bindTrustedStage(f.agentId, f.record);
    await first.change(f.agentId, (record) => {
      record.dispatched = true;
      record.dispatchGeneration = 1;
      record.dispatchClaim = { owner: "first-process", generation: 1, until: 100 };
    });
    const reopened = store();
    expect(await reopened.read(f.agentId)).toMatchObject({
      input: f.record.input,
      dispatched: true,
      dispatchClaim: { owner: "first-process", generation: 1 },
    });
  });

  it("shares one durable stage claim across concurrent aliases/providers", async () => {
    const f = fresh();
    const a = store();
    const b = store();
    const alias = `${f.agentId}-other-run`;
    await Promise.all([
      a.bindTrustedStage(f.agentId, f.record),
      b.bindTrustedStage(alias, f.record),
    ]);
    const runner = new DbFakeRunner(a, f.agentId);
    let release!: () => void;
    runner.beforeStart = () =>
      new Promise<void>((done) => {
        release = done;
      });
    const first = new PiCloudAgentProvider(a, runner, () => 1).launch(
      { idempotencyKey: f.agentId, prompt: f.record.input },
      context,
    );
    await until(() => Boolean(release));
    const duplicate = await new PiCloudAgentProvider(b, runner, () => 1).launch(
      { idempotencyKey: alias, prompt: f.record.input },
      context,
    );
    expect(duplicate.status).toBe("running");
    expect(runner.lookups).toBe(0);
    release();
    await first;
    expect(runner.starts).toBe(1);
    expect((await b.read(alias))?.dispatchGeneration).toBe(1);
  });

  it("rejects conflicting target/role or alias rebinding without replacing a durable stage", async () => {
    const f = fresh();
    const records = store();
    await records.bindTrustedStage(f.agentId, f.record);
    await expect(
      records.bindTrustedStage(`${f.agentId}-wrong`, { ...f.record, botId: "other-bot" }),
    ).rejects.toThrow();
    const other = fresh();
    await expect(records.bindTrustedStage(f.agentId, other.record)).rejects.toThrow();
    expect(await records.read(f.agentId)).toEqual(f.record);
    expect(await records.read(`${f.agentId}-wrong`)).toBeUndefined();
  });

  it("rolls back failed callbacks and refuses binding edits or uncertainty resets", async () => {
    const f = fresh();
    const records = store();
    await records.bindTrustedStage(f.agentId, f.record);
    await expect(
      records.change(f.agentId, (record) => {
        record.cancelRequested = true;
        throw new Error("fake callback fault");
      }),
    ).rejects.toThrow();
    expect((await store().read(f.agentId))?.cancelRequested).toBe(false);
    await expect(
      records.change(f.agentId, (record) => {
        record.threadId = "other-thread";
      }),
    ).rejects.toThrow();
    await records.change(f.agentId, (record) => {
      record.state = "unknown";
      record.reason = "execution-unknown";
    });
    await expect(
      records.change(f.agentId, (record) => {
        record.state = "completed";
      }),
    ).rejects.toThrow();
    expect((await store().read(f.agentId))?.state).toBe("unknown");
  });

  it("serializes cancellation and revocation across connections without lost updates", async () => {
    const f = fresh();
    const a = store();
    const b = store();
    await a.bindTrustedStage(f.agentId, f.record);
    await Promise.all([
      a.change(f.agentId, (record) => {
        record.cancelRequested = true;
      }),
      b.change(f.agentId, (record) => {
        record.sharingAllowed = false;
      }),
    ]);
    expect(await store().read(f.agentId)).toMatchObject({
      cancelRequested: true,
      sharingAllowed: false,
    });
    await expect(
      a.change(f.agentId, (record) => {
        record.cancelRequested = false;
      }),
    ).rejects.toThrow();
    await expect(
      b.change(f.agentId, (record) => {
        record.sharingAllowed = true;
      }),
    ).rejects.toThrow();
  });

  it("keeps the cancellation tombstone after reopening and resolving a new alias", async () => {
    const f = fresh();
    const records = store();
    await records.bindTrustedStage(f.agentId, f.record);
    const runner = new DbFakeRunner(records, f.agentId);
    expect(
      (await new PiCloudAgentProvider(records, runner, () => 1).cancel(f.agentId, context)).status,
    ).toBe("cancelled");
    const reopened = store();
    const alias = `${f.agentId}-new-run`;
    await reopened.bindTrustedStage(alias, f.record);
    expect(
      (
        await new PiCloudAgentProvider(reopened, runner, () => 1).launch(
          { idempotencyKey: alias, prompt: f.record.input },
          context,
        )
      ).status,
    ).toBe("cancelled");
    expect(runner.starts).toBe(0);
    expect((await reopened.read(alias))?.stopConfirmed).toBe(true);
  });

  it("cancels a committed pending start from another connection before any fake execution", async () => {
    const f = fresh();
    const a = store();
    const b = store();
    await a.bindTrustedStage(f.agentId, f.record);
    const runner = new DbFakeRunner(a, f.agentId);
    let release!: () => void;
    runner.beforeStart = () =>
      new Promise<void>((done) => {
        release = done;
      });
    const first = new PiCloudAgentProvider(a, runner, () => 1).launch(
      { idempotencyKey: f.agentId, prompt: f.record.input },
      context,
    );
    await until(() => Boolean(release));
    expect(
      (await new PiCloudAgentProvider(b, runner, () => 1).cancel(f.agentId, context)).status,
    ).toBe("cancelled");
    release();
    expect((await first).status).toBe("cancelled");
    expect(runner.starts).toBe(0);
    expect(await store().read(f.agentId)).toMatchObject({
      dispatched: true,
      cancelRequested: true,
      state: "cancelled",
      stopConfirmed: true,
    });
  });

  it("persists both stages and authorizes only the fixed recipient's read-only handoff", async () => {
    const f = fresh();
    const records = store();
    await records.bindTrustedStage(f.agentId, f.record);
    const runner = new DbFakeRunner(records, f.agentId);
    const provider = new PiCloudAgentProvider(records, runner, () => 1);
    await provider.launch({ idempotencyKey: f.agentId, prompt: f.record.input }, context);
    runner.states.set(f.record.handle, { kind: "completed", text: "Generalized design original" });
    await provider.get(f.agentId, context);
    const original = (await provider.readResult(f.agentId, context)).original!;
    const reviewer = { ...context, botId: "review-bot", threadId: "review-thread" };
    const before = await records.read(f.agentId);
    const target = {
      workId: f.record.workId,
      stage: "design" as const,
      originalHash: original.hash,
    };
    expect(await provider.readHandoff(f.agentId, reviewer, target)).toMatchObject({
      original: { text: original.text },
      accepted: false,
    });
    expect(await records.read(f.agentId)).toEqual(before);
    await expect(
      provider.readHandoff(f.agentId, { ...reviewer, threadId: "wrong" }, target),
    ).rejects.toThrow();
    const reviewId = `${f.agentId}-review`;
    const review = binding({
      ...f.record,
      handle: `${f.record.handle}-review`,
      stage: "review",
      botId: "review-bot",
      threadId: "review-thread",
      input: original.text,
      targetHash: original.hash,
      returnTo: { botId: "design-bot", threadId: "design-thread" },
    });
    await records.bindTrustedStage(reviewId, review);
    const reviewRunner = new DbFakeRunner(records, reviewId);
    const reviewProvider = new PiCloudAgentProvider(records, reviewRunner, () => 1);
    await reviewProvider.launch({ idempotencyKey: reviewId, prompt: original.text }, reviewer);
    reviewRunner.states.set(review.handle, {
      kind: "completed",
      text: "Generalized review original",
      targetHash: original.hash,
    });
    await reviewProvider.get(reviewId, reviewer);
    const result = (await reviewProvider.readResult(reviewId, reviewer)).original!;
    const reopened = new PiCloudAgentProvider(store(), reviewRunner, () => 1);
    expect(
      await reopened.readHandoff(
        reviewId,
        { ...context, threadId: "design-thread" },
        {
          workId: f.record.workId,
          stage: "review",
          originalHash: result.hash,
          targetHash: original.hash,
        },
      ),
    ).toMatchObject({ original: { text: "Generalized review original" }, accepted: false });
    expect(runner.starts).toBe(1);
    expect(reviewRunner.starts).toBe(1);
  });

  it("survives an actual node-process exit at the committed dispatch boundary without relaunch", async () => {
    const f = fresh();
    const child = spawnSync(
      process.execPath,
      [
        fileURLToPath(new URL("../../../node_modules/tsx/dist/cli.mjs", import.meta.url)),
        fileURLToPath(new URL("./pi-stage-postgres.crash-fixture.ts", import.meta.url)),
        JSON.stringify(f),
      ],
      { env: process.env, encoding: "utf8", timeout: 15_000 },
    );
    expect(child.status).toBe(23);
    const records = store();
    expect(await records.read(f.agentId)).toMatchObject({
      dispatched: true,
      dispatchGeneration: 1,
    });
    const runner = new DbFakeRunner(records, f.agentId);
    const provider = new PiCloudAgentProvider(records, runner, () => 60_002);
    expect(
      (await provider.launch({ idempotencyKey: f.agentId, prompt: f.record.input }, context))
        .status,
    ).toBe("failed");
    expect(await provider.readResult(f.agentId, context)).toMatchObject({
      executionState: "unknown",
      stopConfirmed: false,
    });
    runner.states.set(f.record.handle, { kind: "completed", text: "Late original" });
    expect((await provider.get(f.agentId, context)).status).toBe("failed");
    expect((await store().read(f.agentId))?.original?.text).toBe("Late original");
    expect(runner.starts).toBe(0);
  });

  it("enforces real row security without scope and across owners/spaces", async () => {
    const f = fresh();
    await store().bindTrustedStage(f.agentId, f.record);
    const role = await sql(
      "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname=current_user",
    );
    expect(role.rows[0]).toEqual({ rolsuper: false, rolbypassrls: false });
    expect(Number((await sql("SELECT count(*) AS count FROM pi_work_states")).rows[0]?.count)).toBe(
      0,
    );
    expect(
      await new PostgresPiStageStore(new IsolatedPsqlPool(), { ...scope, userId: "other" }).read(
        f.agentId,
      ),
    ).toBeUndefined();
    expect(
      await new PostgresPiStageStore(new IsolatedPsqlPool(), { ...scope, spaceId: "other" }).read(
        f.agentId,
      ),
    ).toBeUndefined();
    await sql("DELETE FROM pi_work_states");
    expect(await store().read(f.agentId)).toBeDefined();
  });

  it("retains tombstones and alias identity even with the runtime's broad table grants", async () => {
    const f = fresh();
    const records = store();
    await records.bindTrustedStage(f.agentId, f.record);
    await records.change(f.agentId, (record) => {
      record.cancelRequested = true;
    });
    const client = await new IsolatedPsqlPool().connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT set_config('app.user_id', $1, true), set_config('app.space_id', $2, true)",
        [scope.userId, scope.spaceId],
      );
      await client.query('DELETE FROM pi_stage_bindings WHERE "agentId"=$1', [f.agentId]);
      await client.query('UPDATE pi_stage_bindings SET stage=$2 WHERE "agentId"=$1', [
        f.agentId,
        "review",
      ]);
      await client.query('DELETE FROM pi_work_states WHERE "workId"=$1', [f.record.workId]);
      await client.query("COMMIT");
    } finally {
      client.release();
    }
    expect(await store().read(f.agentId)).toMatchObject({ stage: "design", cancelRequested: true });
  });

  it("review regression: rejects one runner handle assigned to different works", async () => {
    const a = fresh();
    const b = fresh();
    b.record.handle = a.record.handle;
    const records = store();
    await records.bindTrustedStage(a.agentId, a.record);
    await expect(records.bindTrustedStage(b.agentId, b.record)).rejects.toThrow();
  });

  it("review regression: cannot escape UNKNOWN through running before completed", async () => {
    const f = fresh();
    const records = store();
    await records.bindTrustedStage(f.agentId, f.record);
    await records.change(f.agentId, (record) => {
      record.dispatched = true;
      record.dispatchGeneration = 1;
      record.state = "unknown";
    });
    await expect(
      records.change(f.agentId, (record) => {
        record.state = "running";
      }),
    ).rejects.toThrow();
    await expect(
      records.change(f.agentId, (record) => {
        record.state = "reserved";
      }),
    ).rejects.toThrow();
    expect((await store().read(f.agentId))?.state).toBe("unknown");
  });

  it("review regression: cannot clear reconciliationExpired through false or deletion", async () => {
    const f = fresh();
    const records = store();
    await records.bindTrustedStage(f.agentId, f.record);
    await records.change(f.agentId, (record) => {
      record.state = "unknown";
      record.reconciliationExpired = true;
    });
    await expect(
      records.change(f.agentId, (record) => {
        record.reconciliationExpired = false;
      }),
    ).rejects.toThrow();
    await expect(
      records.change(f.agentId, (record) => {
        record.reconciliationExpired = undefined;
      }),
    ).rejects.toThrow();
    expect((await store().read(f.agentId))?.reconciliationExpired).toBe(true);
  });

  it("enforces the runner handle bijection across stages and tenant scopes", async () => {
    const a = fresh();
    const records = store();
    await records.bindTrustedStage(a.agentId, a.record);
    await expect(
      records.bindTrustedStage(`${a.agentId}-review`, {
        ...a.record,
        stage: "review",
        targetHash: "target",
        botId: "review-bot",
        threadId: "review-thread",
      }),
    ).rejects.toThrow();
    await expect(
      records.bindTrustedStage(`${a.agentId}-new-handle`, {
        ...a.record,
        handle: `${a.record.handle}-new`,
      }),
    ).rejects.toThrow();
    const b = fresh();
    b.record.ownerId = "other";
    b.record.handle = a.record.handle;
    await expect(
      new PostgresPiStageStore(new IsolatedPsqlPool(), {
        ...scope,
        userId: "other",
      }).bindTrustedStage(b.agentId, b.record),
    ).rejects.toThrow();
    expect(await records.read(a.agentId)).toEqual(a.record);
  });

  it("keeps namespaces separate and refuses to operate through the wrong configured runner", async () => {
    const a = fresh();
    const b = fresh();
    b.record.handle = a.record.handle;
    b.record.runnerNamespace = "separate-runner";
    const records = store();
    await records.bindTrustedStage(a.agentId, a.record);
    await records.bindTrustedStage(b.agentId, b.record);
    const runner = new DbFakeRunner(records, a.agentId);
    const provider = new PiCloudAgentProvider(records, runner, () => 1);
    await expect(provider.cancel(b.agentId, context)).rejects.toThrow();
    expect(runner.starts).toBe(0);
    expect(runner.states.size).toBe(0);
    expect((await records.read(b.agentId))?.cancelRequested).toBe(false);
  });

  it("refuses canonical handle updates/deletes and JSON handle drift with broad runtime grants", async () => {
    const a = fresh();
    const b = fresh();
    const records = store();
    await records.bindTrustedStage(a.agentId, a.record);
    await records.bindTrustedStage(b.agentId, b.record);
    await scopedSql("UPDATE pi_stage_handles SET handle=$2 WHERE handle=$1", [
      a.record.handle,
      "changed",
    ]);
    await scopedSql("DELETE FROM pi_stage_handles WHERE handle=$1", [a.record.handle]);
    expect((await records.read(a.agentId))?.handle).toBe(a.record.handle);
    await scopedSql(
      "UPDATE pi_work_states SET stages=jsonb_set(stages, '{design,handle}', to_jsonb($2::text)) WHERE \"workId\"=$1",
      [a.record.workId, b.record.handle],
    );
    await expect(records.read(a.agentId)).rejects.toThrow();
    expect((await records.read(b.agentId))?.handle).toBe(b.record.handle);
  });

  it("keeps UNKNOWN closed even through a confirmed cancellation intermediate", async () => {
    const f = fresh();
    const records = store();
    await records.bindTrustedStage(f.agentId, f.record);
    await records.change(f.agentId, (record) => {
      record.dispatched = true;
      record.dispatchGeneration = 1;
      record.state = "unknown";
      record.reconciliationExpired = true;
    });
    await expect(
      records.change(f.agentId, (record) => {
        record.state = "cancelled";
      }),
    ).rejects.toThrow();
    await records.change(f.agentId, (record) => {
      record.state = "cancelled";
      record.stopConfirmed = true;
    });
    await expect(
      records.change(f.agentId, (record) => {
        record.state = "running";
      }),
    ).rejects.toThrow();
    await expect(
      records.change(f.agentId, (record) => {
        record.state = "completed";
        record.original = {
          text: "Original",
          hash: createHash("sha256").update("Original").digest("hex"),
        };
      }),
    ).rejects.toThrow();
    await expect(
      records.change(f.agentId, (record) => {
        record.reconciliationExpired = false;
      }),
    ).rejects.toThrow();
    expect(await store().read(f.agentId)).toMatchObject({
      state: "cancelled",
      stopConfirmed: true,
      reconciliationExpired: true,
    });
  });

  it("rejects cross-scope INSERT/UPDATE while documenting that same-scope raw JSON is trusted", async () => {
    const f = fresh();
    const records = store();
    await records.bindTrustedStage(f.agentId, f.record);
    await records.change(f.agentId, (record) => {
      record.dispatched = true;
      record.dispatchGeneration = 1;
      record.state = "unknown";
    });
    await expect(
      records.change(f.agentId, (record) => {
        record.state = "running";
      }),
    ).rejects.toThrow();
    // RLS is tenant isolation, NOT a transition/approval verifier: the runtime
    // credential can issue direct same-scope JSON updates. Product paths must use the store.
    await scopedSql(
      "UPDATE pi_work_states SET stages=jsonb_set(stages, '{design,state}', '\"running\"'::jsonb) WHERE \"workId\"=$1",
      [f.record.workId],
    );
    expect((await records.read(f.agentId))?.state).toBe("running");
    const empty = randomUUID();
    await scopedSql(
      'INSERT INTO pi_work_states (id, "ownerId", "spaceId", "workId") VALUES ($1,$2,$3,$4)',
      [empty, scope.userId, scope.spaceId, empty],
    );
    await expect(
      scopedSql('UPDATE pi_work_states SET "ownerId"=$2 WHERE id=$1', [empty, "other"]),
    ).rejects.toThrow();
    await expect(
      scopedSql(
        'INSERT INTO pi_work_states (id, "ownerId", "spaceId", "workId") VALUES ($1,$2,$3,$4)',
        [randomUUID(), "other", scope.spaceId, randomUUID()],
      ),
    ).rejects.toThrow();
    const foreign = fresh();
    foreign.record.ownerId = "other";
    const foreignStore = new PostgresPiStageStore(new IsolatedPsqlPool(), {
      ...scope,
      userId: "other",
    });
    await foreignStore.bindTrustedStage(foreign.agentId, foreign.record);
    await scopedSql(
      "UPDATE pi_work_states SET stages=jsonb_set(stages, '{design,approvalValid}', 'false'::jsonb) WHERE \"workId\"=$1",
      [foreign.record.workId],
    );
    expect((await foreignStore.read(foreign.agentId))?.approvalValid).toBe(true);
  });

  it("uses a per-work PostgreSQL row lock, allowing unrelated work to progress", async () => {
    const a = fresh();
    const b = fresh();
    const records = store();
    await records.bindTrustedStage(a.agentId, a.record);
    await records.bindTrustedStage(b.agentId, b.record);
    const held = await new IsolatedPsqlPool().connect();
    try {
      await held.query("BEGIN");
      await held.query(
        "SELECT set_config('app.user_id', $1, true), set_config('app.space_id', $2, true)",
        [scope.userId, scope.spaceId],
      );
      await held.query('SELECT id FROM pi_work_states WHERE "workId"=$1 FOR UPDATE', [
        a.record.workId,
      ]);
      let finished = false;
      const blocked = store()
        .change(a.agentId, (record) => {
          record.cancelRequested = true;
        })
        .then(() => {
          finished = true;
        });
      let waiting = false;
      for (let index = 0; index < 50; index++) {
        waiting =
          Number(
            (
              await sql(
                "SELECT count(*) AS count FROM pg_stat_activity WHERE usename='pi_stage_app' AND wait_event_type='Lock'",
                true,
              )
            ).rows[0]?.count,
          ) > 0;
        if (waiting) break;
        await new Promise((done) => setTimeout(done, 10));
      }
      expect(waiting).toBe(true);
      expect(finished).toBe(false);
      await store().change(b.agentId, (record) => {
        record.cancelRequested = true;
      });
      expect(finished).toBe(false);
      expect((await records.read(b.agentId))?.cancelRequested).toBe(true);
      await held.query("COMMIT");
      await blocked;
      expect((await records.read(a.agentId))?.cancelRequested).toBe(true);
    } finally {
      held.release();
    }
  });
});
