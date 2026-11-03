import type { AgentRuntime, AgentRuntimeEvent, ModelCallObserver } from "@rakazo/adapter-kit";
import type { CloudAgent, PrismaClient } from "@rakazo/db";
import { describe, expect, it, vi } from "vitest";
import { pollCloudAgent } from "./cloud-agent-poll.js";
import type { CloudAgentDeps } from "./cloud-agent-service.js";
import { executeCloudAgentTool } from "./cloud-agent-service.js";
import type { ExecutorDeps } from "./executor-deps.js";
import { PiCloudAgentProvider } from "./pi-cloud-agent.js";
import { binding, context, FakePiRunner, FakeStageStore } from "./pi-cloud-agent.test-fixtures.js";
import { returnFixedPiResult } from "./pi-fixed-return.js";
import type { PiApprovedStage, PiWorkApprovalRegistry } from "./pi-native-admission.js";
import {
  PiNativeAdmission,
  piNativeStageId,
  piResultReferenceText,
  registerPiOwnerStage,
} from "./pi-native-admission.js";
import { PiRunApprovalSource } from "./pi-run-approval-source.js";
import { createPiRuntimeConnection } from "./pi-runtime-connection.js";
import {
  PI_WORK_APPROVAL_KIND,
  PrismaPiWorkApprovalSource,
  piWorkDraftHash,
} from "./pi-work-approval.js";

vi.mock("@rakazo/db", async (original) => ({
  ...(await original<object>()),
  createThreadMessageInTransaction: async (
    tx: { message: { create(args: unknown): unknown } },
    data: unknown,
  ) => tx.message.create({ data }),
  appendEventInTransaction: async () => ({ seq: 1 }),
}));

/** Fake receipt boundary. Production authenticated registration, stable Run lineage
 * and receipt persistence are NOT supplied/tested by this adapter-level fixture. */
class Approvals implements PiWorkApprovalRegistry {
  readonly runs = new Map<string, PiApprovedStage>();
  readonly slots = new Map<string, PiApprovedStage>();
  readonly decisions = new Map<string, string>();
  async forStage(id: string) {
    return structuredClone(this.slots.get(id));
  }
  async forRun(id: string) {
    return structuredClone(this.runs.get(id));
  }
  async register(id: string, receipt: PiApprovedStage) {
    const key = piNativeStageId(receipt.record);
    const decisionKey = `${receipt.approvedBy}:${receipt.decisionId}:${receipt.record.stage}`;
    const priorWork = this.decisions.get(decisionKey);
    if (priorWork && priorWork !== key) throw new Error("immutable decision");
    const old = this.slots.get(key);
    if (old && JSON.stringify(old) !== JSON.stringify(receipt)) throw new Error("immutable work");
    const run = this.runs.get(id);
    if (run && JSON.stringify(run) !== JSON.stringify(receipt)) throw new Error("immutable run");
    this.decisions.set(decisionKey, key);
    this.slots.set(key, structuredClone(receipt));
    this.runs.set(id, structuredClone(receipt));
  }
}

type Row = Record<string, any>;
function fixture() {
  const records = new FakeStageStore();
  const registrar = {
    read: records.read.bind(records),
    change: records.change.bind(records),
    bindTrustedStage: async (id: string, record: Parameters<FakeStageStore["bind"]>[1]) =>
      records.bind(id, record),
  };
  const approvals = new Approvals();
  const admission = new PiNativeAdmission(approvals, registrar, () => 1_000);
  const runner = new FakePiRunner(records);
  const provider = new PiCloudAgentProvider(registrar, runner, () => 1_000);
  const bots = [
    {
      id: "design-bot",
      name: "Design",
      thread: { id: "design-thread" },
      disabledBuiltinTools: [] as string[],
    },
    {
      id: "review-bot",
      name: "Review",
      thread: { id: "review-thread" },
      disabledBuiltinTools: [] as string[],
    },
  ];
  const runs = new Map<string, Row>();
  const agents = new Map<string, CloudAgent>();
  const messages = new Map<string, Row>();
  const effects = new Map<string, Row>();
  let member = true;
  const activeRun = (id: string, botId = "design-bot", threadId = "design-thread") => {
    runs.set(id, {
      ...runs.get(id),
      id,
      botId,
      threadId,
      spaceId: "space",
      userId: "owner",
      status: "running",
      leaseOwner: "worker",
      leaseFence: 1,
      leaseExpiresAt: new Date(Date.now() + 60_000),
    });
    return { id, threadId };
  };
  activeRun("origin-run");
  const matches = (row: Row, where: Row = {}) =>
    Object.entries(where).every(([k, v]) =>
      typeof v === "object" && v !== null ? true : row[k] === v,
    );
  const apply = (row: Row, data: Row) => {
    for (const [k, v] of Object.entries(data))
      row[k] = v && typeof v === "object" && "increment" in v ? row[k] + v.increment : v;
    return structuredClone(row);
  };
  const tx: Row = {
    $queryRaw: async () => [],
    externalEffect: {
      findUnique: async ({ where }: Row) => structuredClone(effects.get(where.id) ?? null),
      findMany: async ({ where, take }: Row) =>
        [...effects.values()]
          .filter(
            (e) =>
              e.spaceId === where.spaceId &&
              e.kind === where.kind &&
              e.status === where.status &&
              where.OR.some((q: Row) => e.request.stageIds[q.request.path[1]] === q.request.equals),
          )
          .slice(0, take)
          .map((e) => structuredClone(e)),
      create: async ({ data }: Row) => {
        if (effects.has(data.id)) throw new Error("unique");
        effects.set(data.id, structuredClone(data));
        return structuredClone(data);
      },
      updateMany: async ({ where, data }: Row) => {
        const e = effects.get(where.id);
        if (!e || !matches(e, where)) return { count: 0 };
        apply(e, data);
        return { count: 1 };
      },
    },
    thread: {
      update: async ({ where }: Row) => ({ id: where.id }),
      updateMany: async () => ({ count: 1 }),
      findFirst: async ({ where }: Row) =>
        bots.some((b) => b.thread.id === where.id && (!where.botId || b.id === where.botId))
          ? {
              id: where.id,
              userId: "owner",
              spaceId: "space",
              botId: bots.find((b) => b.thread.id === where.id)!.id,
            }
          : null,
    },
    bot: {
      findMany: async () => structuredClone(bots),
      findFirst: async ({ where }: Row) =>
        bots.find((b) => b.id === where.id && (!where.thread || b.thread.id === where.thread.id)) ??
        null,
    },
    spaceMember: { findUnique: async () => (member ? { id: "member" } : null) },
    run: {
      findFirst: async ({ where }: Row) =>
        [...runs.values()].find((r) => matches(r, where)) ?? null,
      create: async ({ data }: Row) => {
        const row = { ...data, id: `wake-${runs.size}` };
        runs.set(row.id, row);
        return row;
      },
    },
    task: { create: async ({ data }: Row) => ({ ...data, id: "task" }) },
    message: {
      create: async ({ data }: Row) => {
        const row = { ...data, id: `message-${messages.size}` };
        messages.set(row.id, row);
        return row;
      },
      findFirst: async ({ where }: Row) =>
        [...messages.values()].find((m) => matches(m, where)) ?? null,
      findUnique: async ({ where }: Row) =>
        where.id
          ? (messages.get(where.id) ?? null)
          : ([...messages.values()].find((m) => matches(m, where.threadId_clientNonce)) ?? null),
      update: async ({ where, data }: Row) => apply(messages.get(where.id)!, data),
    },
    cloudAgent: {
      findUnique: async ({ where }: Row) =>
        structuredClone([...agents.values()].find((a) => matches(a, where)) ?? null),
      findUniqueOrThrow: async ({ where }: Row) => structuredClone(agents.get(where.id)!),
      findFirst: async ({ where }: Row) =>
        structuredClone([...agents.values()].find((a) => matches(a, where)) ?? null),
      create: async ({ data }: Row) => {
        if (
          [...agents.values()].some((a) => a.operationKey === data.operationKey || a.id === data.id)
        )
          throw new Error("unique");
        const row = {
          status: "running",
          createdAt: new Date(),
          url: "",
          remoteId: null,
          launchDispatched: false,
          cancelRequested: false,
          followup: null,
          followupDispatchedAt: null,
          generation: 0,
          wakeGeneration: -1,
          version: 0,
          leaseToken: null,
          leaseExpiresAt: null,
          nextPollAt: new Date(),
          errorCount: 0,
          ...data,
        } as CloudAgent;
        agents.set(row.id, row);
        return structuredClone(row);
      },
      update: async ({ where, data }: Row) => apply(agents.get(where.id)!, data),
      updateMany: async ({ where, data }: Row) => {
        const a = agents.get(where.id);
        if (
          !a ||
          (where.version !== undefined && a.version !== where.version) ||
          (where.leaseToken !== undefined && a.leaseToken !== where.leaseToken) ||
          (where.OR && a.leaseExpiresAt && a.leaseExpiresAt > new Date())
        )
          return { count: 0 };
        apply(a, data);
        return { count: 1 };
      },
    },
  };
  // Serial fake transactions, NOT PostgreSQL locking/concurrency evidence.
  let tail = Promise.resolve();
  const prisma = {
    ...tx,
    $transaction: (fn: (tx: Row) => unknown) => {
      const next = tail.then(async () => {
        const maps = [runs, agents, messages, effects];
        const snapshots = maps.map((map) => structuredClone([...map.entries()]));
        try {
          return await fn(tx);
        } catch (error) {
          maps.forEach((map, index) => {
            map.clear();
            for (const [key, value] of snapshots[index]!) map.set(key, value as never);
          });
          throw error;
        }
      });
      tail = next.then(
        () => undefined,
        () => undefined,
      );
      return next;
    },
  } as unknown as PrismaClient;
  const deps: CloudAgentDeps = {
    prisma,
    piAdmission: admission,
    cloudAgent: { key: "fake-pi", spaceId: "space", provider },
    events: { notify: vi.fn(async () => undefined) },
    jobs: {
      enqueue: vi.fn(async () => undefined),
      cancel: async () => undefined,
      close: async () => undefined,
    },
    runLease: { runId: "origin-run", owner: "worker", fence: 1 },
  };
  const use = (run: { id: string; threadId: string }, botId = "design-bot") => {
    deps.runLease = { runId: run.id, owner: "worker", fence: 1 };
    return { ...context, botId };
  };
  const register = async (record = binding()) =>
    registerPiOwnerStage(
      {
        prisma,
        approvals,
        runnerNamespace: "fake-pi",
        now: () => 1_000,
        authenticateOwner: async () => ({ userId: "owner", spaceId: "space" }),
      },
      { approve: true, runId: "origin-run", decisionId: record.approvalRef, record },
    );
  return {
    records,
    registrar,
    effects,
    approvals,
    admission,
    runner,
    provider,
    bots,
    runs,
    agents,
    messages,
    deps,
    activeRun,
    use,
    register,
    setMember: (value: boolean) => {
      member = value;
    },
    poll: (id: string) => pollCloudAgent(deps, { agentId: id }),
    launch: (
      run = { id: "origin-run", threadId: "design-thread" },
      botId = "design-bot",
      operationId = "effect",
      prompt = "Generalized task",
    ) =>
      executeCloudAgentTool(deps, { ...use(run, botId), operationId }, run, "cloud_agent_launch", {
        prompt,
      }),
  };
}

async function approvedMessage(f: ReturnType<typeof fixture>, id: string) {
  const record = (await f.records.read(id))!;
  return `Pi ${record.stage} original: ${id}; sha256 ${record.original!.hash}. Use cloud_agent_result to read it. Acceptance has not been determined.`;
}

describe("server-registered Pi admission and fixed return (fake boundaries only)", () => {
  it("registers an explicit authenticated owner decision and shares a native slot across operation keys and Runs", async () => {
    const f = fixture();
    const saved = await f.register();
    const run2 = f.activeRun("second");
    await f.approvals.register("second", (await f.approvals.forRun("origin-run"))!);
    const first = await f.launch();
    const second = await f.launch(run2, "design-bot", "different-operation");
    expect(first).toHaveProperty("id", saved.id);
    expect(second).toHaveProperty("id", saved.id);
    expect(f.agents.size).toBe(1);
    expect(f.messages.size).toBe(1);
    await f.poll(saved.id);
    await f.poll(saved.id);
    expect(f.runner.starts).toBe(1);
  });

  it("serializes concurrent native admission in the fake transaction and creates one card", async () => {
    const f = fixture();
    await f.register();
    await Promise.all([
      f.launch(undefined, undefined, "one"),
      f.launch(undefined, undefined, "two"),
    ]);
    expect(f.agents.size).toBe(1);
    expect(f.messages.size).toBe(1);
  });

  it("uses existing native completion and fixed message delivery for both stages, with explicit fake receipt lineage", async () => {
    const f = fixture();
    const design = await f.register();
    await f.launch();
    await f.poll(design.id);
    f.runner.states.set("work-design", { kind: "completed", text: "Original design" });
    await f.poll(design.id);
    // The native poll clears launchRequest; the immutable registered stage is
    // still the source of approved input and dedup identity.
    expect(((await f.launch(undefined, undefined, "after-completion")) as { id: string }).id).toBe(
      design.id,
    );
    expect(f.runner.starts).toBe(1);
    const wake = [...f.runs.values()].find((r) => r.trigger === "cloud_agent")!;
    expect(wake).not.toHaveProperty("sourceMessageId");
    const designRun = f.activeRun(wake.id);
    const designReceipt = (await f.approvals.forRun("origin-run"))!;
    // Production mapping from this persisted continuation to the finite consent is
    // still absent. We explicitly supply that trusted boundary, not model prose.
    await f.approvals.register(designRun.id, designReceipt);
    let caller = f.use(designRun);
    const designMessage = await approvedMessage(f, design.id);
    const send = () =>
      returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        caller,
        designRun,
        { id: "design-bot", name: "Design" },
        { intent: "result", bot_id: "review-bot", confirm_name: "review", message: designMessage },
      );
    expect(await send()).toHaveProperty("ok", true);
    expect(await send()).toHaveProperty("ok", true);
    const deliveries = [...f.messages.values()].filter((m) =>
      m.clientNonce?.startsWith("bot-message:pi-result:"),
    );
    expect(deliveries).toHaveLength(1);
    expect(JSON.stringify(deliveries)).not.toContain("Original design");
    expect(JSON.stringify(deliveries)).not.toContain("invented supplement");
    const reviewWake = [...f.runs.values()].find((r) => r.trigger === "bot_message")!;
    const reviewRun = f.activeRun(reviewWake.id, "review-bot", "review-thread");
    caller = f.use(reviewRun, "review-bot");
    const original = (await f.records.read(design.id))!.original!;
    const reviewRecord = binding({
      stage: "review",
      handle: "work-review",
      botId: "review-bot",
      threadId: "review-thread",
      input: `Review the original design: ${original.text}`,
      targetHash: original.hash,
      returnTo: { botId: "design-bot", threadId: "design-thread" },
    });
    // The same finite consent is supplied through the fake server registry; no
    // additional human/tool approval is manufactured by this admission adapter.
    await f.approvals.register(reviewRun.id, { ...designReceipt, record: reviewRecord });
    expect(
      await executeCloudAgentTool(f.deps, caller, reviewRun, "cloud_agent_result", {
        id: design.id,
      }),
    ).toMatchObject({ original: { text: "Original design" }, accepted: false });
    const review = (await f.launch(
      reviewRun,
      "review-bot",
      "review-effect",
      reviewRecord.input,
    )) as { id: string };
    await f.poll(review.id);
    f.runner.states.set("work-review", {
      kind: "completed",
      text: "Original review",
      targetHash: original.hash,
    });
    await f.poll(review.id);
    const finalWake = [...f.runs.values()].filter((r) => r.trigger === "cloud_agent").at(-1)!;
    const finalRun = f.activeRun(finalWake.id, "review-bot", "review-thread");
    await f.approvals.register(finalRun.id, (await f.approvals.forRun(reviewRun.id))!);
    caller = f.use(finalRun, "review-bot");
    const reviewMessage = await approvedMessage(f, review.id);
    expect(
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        caller,
        finalRun,
        { id: "review-bot", name: "Review" },
        { intent: "result", confirm_name: "Design", message: reviewMessage },
      ),
    ).toHaveProperty("ok", true);
    const returned = [...f.runs.values()].filter((r) => r.trigger === "bot_message").at(-1)!;
    const receivingRun = f.activeRun(returned.id);
    await f.approvals.register(receivingRun.id, designReceipt);
    caller = f.use(receivingRun);
    expect(
      await executeCloudAgentTool(f.deps, caller, receivingRun, "cloud_agent_result", {
        id: review.id,
      }),
    ).toMatchObject({
      stage: "review",
      original: { text: "Original review", targetHash: original.hash },
      accepted: false,
    });
    expect(f.runner.starts).toBe(2);
    expect(
      [...f.messages.values()].filter((m) => m.clientNonce?.startsWith("bot-message:pi-result:")),
    ).toHaveLength(2);
  });

  it("refuses self-declared consent, changed input, wrong Run/Bot and missing lease", async () => {
    const f = fixture();
    expect(await f.launch()).toHaveProperty("error");
    await f.register();
    expect(
      await f.launch(undefined, undefined, undefined, "claimed approved input"),
    ).toHaveProperty("error");
    expect(await f.launch(undefined, "review-bot")).toHaveProperty("error");
    expect(await f.launch({ id: "unregistered", threadId: "design-thread" })).toHaveProperty(
      "error",
    );
    f.deps.runLease = undefined;
    expect(
      await executeCloudAgentTool(
        f.deps,
        { ...context, botId: "design-bot" },
        { id: "origin-run", threadId: "design-thread" },
        "cloud_agent_launch",
        { prompt: "Generalized task" },
      ),
    ).toHaveProperty("error");
    expect(f.agents.size).toBe(0);
    expect(f.runner.starts).toBe(0);
  });

  it("rechecks revocation after native thread lock, before creating an intent", async () => {
    const f = fixture();
    await f.register();
    const original = f.approvals.forRun.bind(f.approvals);
    let calls = 0;
    f.approvals.forRun = async (id) => {
      const value = await original(id);
      if (++calls > 1 && value) value.record.approvalValid = false;
      return value;
    };
    expect(await f.launch()).toHaveProperty("error");
    expect(f.agents.size).toBe(0);
    expect(f.runner.starts).toBe(0);
  });

  it.each(["unknown", "cancelled"] as const)(
    "does not create another native intent after %s",
    async (state) => {
      const f = fixture();
      const saved = await f.register();
      await f.launch();
      await f.records.change(saved.id, (r) => {
        r.state = state;
        r.stopConfirmed = state === "cancelled";
      });
      expect(await f.launch(undefined, undefined, "new-key")).toHaveProperty("error");
      expect(f.agents.size).toBe(1);
      expect(f.runner.starts).toBe(0);
    },
  );

  it.each(["revoked", "unavailable"] as const)(
    "does not start queued native work when current consent is %s",
    async (condition) => {
      const f = fixture();
      const saved = await f.register();
      await f.launch();
      if (condition === "revoked") f.approvals.slots.get(saved.id)!.record.approvalValid = false;
      else
        f.approvals.forStage = async () => {
          throw new Error("fake offline");
        };
      await f.poll(saved.id);
      expect(f.runner.starts).toBe(0);
      expect(f.runner.cancelCalls).toBe(1);
      expect(f.agents.get(saved.id)?.status).toBe("cancelled");
      expect((await f.records.read(saved.id))?.stopConfirmed).toBe(true);
    },
  );

  it("refuses owner registration without current membership, authentication, or an immutable decision", async () => {
    const f = fixture();
    f.setMember(false);
    await expect(f.register()).rejects.toThrow("not authorized");
    f.setMember(true);
    await f.register();
    await expect(f.register(binding({ input: "changed work" }))).rejects.toThrow("not authorized");
    await expect(
      registerPiOwnerStage(
        {
          prisma: f.deps.prisma,
          approvals: f.approvals,
          authenticateOwner: async () => undefined,
          runnerNamespace: "fake-pi",
          now: () => 1_000,
        },
        { approve: true, decisionId: "finite-grant-v1", runId: "origin-run", record: binding() },
      ),
    ).rejects.toThrow("not authorized");
  });

  it("does not reuse a decision/stage for another work or bind a Run to another stage", async () => {
    const f = fixture();
    await f.register();
    await expect(f.register(binding({ workId: "another-work" }))).rejects.toThrow("not authorized");
  });

  it("does not send a result before completion or after membership, sharing or finite approval revocation", async () => {
    for (const revoked of ["pending", "member", "sharing", "approval"] as const) {
      const f = fixture();
      const saved = await f.register();
      await f.launch();
      await f.poll(saved.id);
      if (revoked !== "pending") {
        f.runner.states.set("work-design", { kind: "completed", text: "Original" });
        await f.poll(saved.id);
      }
      if (revoked === "member") f.setMember(false);
      if (revoked === "sharing")
        await f.records.change(saved.id, (r) => {
          r.sharingAllowed = false;
        });
      if (revoked === "approval")
        await f.records.change(saved.id, (r) => {
          r.approvalValid = false;
        });
      const run = { id: "origin-run", threadId: "design-thread" };
      const caller = f.use(run);
      expect(
        await returnFixedPiResult(
          f.deps as ExecutorDeps,
          f.provider,
          caller,
          run,
          { id: "design-bot", name: "Design" },
          { intent: "result" },
        ),
      ).toHaveProperty("error");
      expect([...f.messages.values()].filter((m) => m.clientNonce)).toHaveLength(0);
    }
  });

  it("rejects broader deadlines or sharing and refuses a changed fixed destination", async () => {
    const f = fixture();
    await expect(f.register(binding({ workUntil: 10_000_000 }))).rejects.toThrow("not authorized");
    await expect(f.register(binding({ sharingAllowed: false }))).rejects.toThrow("not authorized");
    const saved = await f.register();
    await f.launch();
    await f.poll(saved.id);
    f.runner.states.set("work-design", { kind: "completed", text: "Original" });
    await f.poll(saved.id);
    const run = { id: "origin-run", threadId: "design-thread" };
    const caller = f.use(run);
    const send = (args: Row) =>
      returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        caller,
        run,
        { id: "design-bot", name: "Design" },
        args,
      );
    expect(await send({ intent: "status" })).toHaveProperty("error");
    expect(await send({ intent: "result", bot_id: "other-bot" })).toHaveProperty("error");
    f.bots[1]!.thread.id = "new-thread";
    expect(
      await send({ intent: "result", message: await approvedMessage(f, saved.id) }),
    ).toHaveProperty("error");
    expect([...f.messages.values()].filter((m) => m.clientNonce)).toHaveLength(0);
  });
  it("fails closed when native polling has no receipt authority", async () => {
    const f = fixture();
    const saved = await f.register();
    await f.launch();
    f.deps.piAdmission = undefined;
    await f.poll(saved.id);
    expect(f.runner.starts).toBe(0);
    expect(f.agents.get(saved.id)?.status).not.toBe("running");
  });
  it.each(["run", "stage"] as const)(
    "does not read a completed original after current %s receipt revocation",
    async (kind) => {
      const f = fixture();
      const saved = await f.register();
      await f.launch();
      await f.poll(saved.id);
      f.runner.states.set("work-design", { kind: "completed", text: "Original" });
      await f.poll(saved.id);
      if (kind === "run") f.approvals.runs.get("origin-run")!.record.approvalValid = false;
      else f.approvals.slots.get(saved.id)!.record.approvalValid = false;
      const run = { id: "origin-run", threadId: "design-thread" };
      const caller = f.use(run);
      const result = await executeCloudAgentTool(f.deps, caller, run, "cloud_agent_result", {
        id: saved.id,
      });
      expect(result).toHaveProperty("error");
      expect(result).not.toHaveProperty("original");
    },
  );
  it.each([
    "wrong-name",
    "both-fields-mismatch",
    "unknown-name",
    "ambiguous-name",
    "different-content",
  ] as const)(
    "refuses a fixed return with %s instead of silently replacing the approved request",
    async (kind) => {
      const f = fixture();
      const saved = await f.register();
      await f.launch();
      await f.poll(saved.id);
      f.runner.states.set("work-design", { kind: "completed", text: "Original" });
      await f.poll(saved.id);
      const original = (await f.records.read(saved.id))!.original!;
      const message = `Pi design original: ${saved.id}; sha256 ${original.hash}. Use cloud_agent_result to read it. Acceptance has not been determined.`;
      const run = { id: "origin-run", threadId: "design-thread" };
      const caller = f.use(run);
      if (kind === "ambiguous-name")
        f.bots.push({
          id: "other",
          name: "Review",
          thread: { id: "other-thread" },
          disabledBuiltinTools: [],
        });
      const result = await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        caller,
        run,
        { id: "design-bot", name: "Design" },
        {
          intent: "result",
          bot_id: kind === "both-fields-mismatch" ? "review-bot" : undefined,
          confirm_name:
            kind === "wrong-name" || kind === "both-fields-mismatch"
              ? "Design"
              : kind === "unknown-name"
                ? "Missing"
                : "Review",
          message: kind === "different-content" ? "Unapproved replacement" : message,
        },
      );
      expect(result).toHaveProperty("error");
      expect([...f.messages.values()].filter((m) => m.clientNonce)).toHaveLength(0);
    },
  );

  it.each(["run", "stage"] as const)(
    "does not emit bytes if %s receipt revokes during a result read",
    async (kind) => {
      const f = fixture();
      const saved = await f.register();
      await f.launch();
      await f.poll(saved.id);
      f.runner.states.set("work-design", { kind: "completed", text: "Original" });
      await f.poll(saved.id);
      const read = f.provider.readForRun.bind(f.provider);
      vi.spyOn(f.provider, "readForRun").mockImplementation(async (...args) => {
        const result = await read(...args);
        if (kind === "run") f.approvals.runs.get("origin-run")!.record.approvalValid = false;
        else f.approvals.slots.get(saved.id)!.record.approvalValid = false;
        return result;
      });
      const run = { id: "origin-run", threadId: "design-thread" };
      const caller = f.use(run);
      const result = await executeCloudAgentTool(f.deps, caller, run, "cloud_agent_result", {
        id: saved.id,
      });
      expect(result).toHaveProperty("error");
      expect(result).not.toHaveProperty("original");
    },
  );
  it.each(["missing", "unavailable", "wrong-work"] as const)(
    "does not read originals with %s current receipt authority",
    async (condition) => {
      const f = fixture();
      const saved = await f.register();
      await f.launch();
      await f.poll(saved.id);
      f.runner.states.set("work-design", { kind: "completed", text: "Original" });
      await f.poll(saved.id);
      if (condition === "missing") f.deps.piAdmission = undefined;
      if (condition === "unavailable")
        f.approvals.forRun = async () => {
          throw new Error("private fake error");
        };
      if (condition === "wrong-work")
        f.approvals.runs.get("origin-run")!.record.workId = "other-work";
      const run = { id: "origin-run", threadId: "design-thread" };
      const caller = f.use(run);
      const result = await executeCloudAgentTool(f.deps, caller, run, "cloud_agent_result", {
        id: saved.id,
      });
      expect(result).toHaveProperty("error");
      expect(result).not.toHaveProperty("original");
      expect(JSON.stringify(result)).not.toContain("private fake error");
    },
  );
});

function lineageFixture(now = () => 1_000) {
  const f = fixture();
  const source = new PiRunApprovalSource(f.deps.prisma, f.approvals, f.records, "fake-pi", now);
  f.deps.piAdmission = new PiNativeAdmission(source, f.registrar, now);
  return { ...f, source };
}
async function finishedDesign() {
  const f = lineageFixture();
  const design = await f.register();
  await f.launch();
  await f.poll(design.id);
  f.runner.states.set("work-design", { kind: "completed", text: "Original design" });
  await f.poll(design.id);
  const wake = [...f.runs.values()].find((r) => r.trigger === "cloud_agent")!;
  const run = f.activeRun(wake.id);
  const caller = f.use(run);
  return { ...f, design, run, caller };
}
async function deliveredDesign() {
  const f = await finishedDesign();
  const result = await returnFixedPiResult(
    f.deps as ExecutorDeps,
    f.provider,
    f.caller,
    f.run,
    { id: "design-bot", name: "Design" },
    { intent: "result", bot_id: "review-bot", message: await approvedMessage(f, f.design.id) },
  );
  expect(result).toHaveProperty("ok", true);
  const wake = [...f.runs.values()].find((r) => r.trigger === "bot_message")!;
  const reviewRun = f.activeRun(wake.id, "review-bot", "review-thread");
  const reviewCaller = f.use(reviewRun, "review-bot");
  return { ...f, reviewRun, reviewCaller };
}
function supplyApprovedReview(f: Awaited<ReturnType<typeof deliveredDesign>>) {
  const original = [...f.records.records.values()].find((r) => r.stage === "design")!.original!;
  const record = binding({
    stage: "review",
    handle: "work-review",
    botId: "review-bot",
    threadId: "review-thread",
    targetHash: original.hash,
    input: `Review the original design: ${original.text}`,
    returnTo: { botId: "design-bot", threadId: "design-thread" },
  });
  const receipt = {
    decisionId: record.approvalRef,
    approvedBy: "owner",
    scope: "generalized-task-and-results" as const,
    record,
  };
  // Fake finite-plan materialization boundary only. No child Run mapping is added.
  f.approvals.slots.set(piNativeStageId(record), receipt);
  return receipt;
}

describe("existing native and peer records resolve Pi child Run authority", () => {
  it("runs design/review and reads the returned review without manually registering any child Run", async () => {
    const f = await deliveredDesign();
    const reviewReceipt = supplyApprovedReview(f);
    expect(await f.source.forRun(f.reviewRun.id)).toMatchObject({
      runAccess: "execute",
      record: { stage: "review" },
    });
    expect(
      await executeCloudAgentTool(f.deps, f.reviewCaller, f.reviewRun, "cloud_agent_result", {
        id: f.design.id,
      }),
    ).toMatchObject({ original: { text: "Original design" }, accepted: false });
    const review = (await f.launch(
      f.reviewRun,
      "review-bot",
      "review-effect",
      reviewReceipt.record.input,
    )) as { id: string };
    await f.poll(review.id);
    f.runner.states.set("work-review", {
      kind: "completed",
      text: "Original review",
      targetHash: reviewReceipt.record.targetHash,
    });
    await f.poll(review.id);
    const wake = [...f.runs.values()].filter((r) => r.trigger === "cloud_agent").at(-1)!;
    const reviewDone = f.activeRun(wake.id, "review-bot", "review-thread");
    const caller = f.use(reviewDone, "review-bot");
    expect(
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        caller,
        reviewDone,
        { id: "review-bot", name: "Review" },
        { intent: "result", confirm_name: "Design", message: await approvedMessage(f, review.id) },
      ),
    ).toHaveProperty("ok", true);
    const returned = [...f.runs.values()].filter((r) => r.trigger === "bot_message").at(-1)!;
    const finalRun = f.activeRun(returned.id);
    const finalCaller = f.use(finalRun);
    expect(await f.source.forRun(finalRun.id)).toMatchObject({
      runAccess: "read",
      record: { stage: "review" },
    });
    expect(
      await executeCloudAgentTool(f.deps, finalCaller, finalRun, "cloud_agent_result", {
        id: review.id,
      }),
    ).toMatchObject({ original: { text: "Original review" }, accepted: false });
    expect(await f.launch(finalRun)).toHaveProperty("error");
    expect(
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        finalCaller,
        finalRun,
        { id: "design-bot", name: "Design" },
        { intent: "result", message: await approvedMessage(f, f.design.id) },
      ),
    ).toHaveProperty("error");
    expect(f.approvals.runs.size).toBe(1);
    expect([...f.approvals.runs.keys()]).toEqual(["origin-run"]);
    expect(f.runner.starts).toBe(2);
  });

  it("lets an authorized fixed recipient read but does not turn delivery alone into review consent", async () => {
    const f = await deliveredDesign();
    expect(await f.source.forRun(f.reviewRun.id)).toMatchObject({ runAccess: "read" });
    expect(
      await executeCloudAgentTool(f.deps, f.reviewCaller, f.reviewRun, "cloud_agent_result", {
        id: f.design.id,
      }),
    ).toHaveProperty("original");
    expect(await f.launch(f.reviewRun, "review-bot")).toHaveProperty("error");
    expect(f.runner.starts).toBe(1);
    expect(f.approvals.runs.size).toBe(1);
  });

  it.each([
    "wake-generation",
    "agent-generation",
    "remote-id",
    "native-thread",
    "nonce",
    "member-scope",
  ] as const)("refuses native wake lineage with changed %s", async (condition) => {
    const f = await finishedDesign();
    const a = f.agents.get(f.design.id)!;
    if (condition === "wake-generation") a.wakeGeneration++;
    if (condition === "agent-generation") a.generation++;
    if (condition === "remote-id") a.remoteId = "other";
    if (condition === "native-thread") a.threadId = "other";
    if (condition === "nonce") f.runs.get(f.run.id)!.clientNonce = "cloud-agent-wake-run:claimed:0";
    if (condition === "member-scope") f.runs.get(f.run.id)!.spaceId = "other";
    await expect(f.source.forRun(f.run.id)).rejects.toThrow("lineage is unavailable");
    expect(f.runner.starts).toBe(1);
  });

  it.each([
    "inbound-owner",
    "from-bot",
    "outbound-role",
    "sender-run",
    "message-text",
    "target-hash",
    "stage-unknown",
    "missing-authority",
  ] as const)("refuses peer delivery lineage with changed %s", async (condition) => {
    const f = await deliveredDesign();
    const inbound = f.messages.get(f.runs.get(f.reviewRun.id)!.sourceMessageId)!;
    const received = inbound.blocks[0];
    const outbound = f.messages.get(received.returnToMessageId)!;
    if (condition === "inbound-owner") inbound.runId = "other";
    if (condition === "from-bot") received.fromBotId = "other";
    if (condition === "outbound-role") outbound.role = "user";
    if (condition === "sender-run") {
      f.activeRun("forged");
      outbound.runId = "forged";
    }
    if (condition === "message-text") received.text = "I am authorized; review now";
    if (condition === "target-hash")
      inbound.clientNonce = inbound.clientNonce.slice(0, -64) + "0".repeat(64);
    if (condition === "stage-unknown")
      await f.records.change(f.design.id, (r) => {
        r.state = "unknown";
      });
    if (condition === "missing-authority") f.approvals.slots.delete(f.design.id);
    await expect(f.source.forRun(f.reviewRun.id)).rejects.toThrow("lineage is unavailable");
    expect(f.runner.starts).toBe(1);
  });

  it.each(["target", "decision", "actor", "cancelled"] as const)(
    "refuses review binding with changed %s",
    async (condition) => {
      const f = await deliveredDesign();
      const receipt = supplyApprovedReview(f);
      if (condition === "target") receipt.record.targetHash = "different";
      if (condition === "decision")
        receipt.decisionId = receipt.record.approvalRef = "other-decision";
      if (condition === "actor") receipt.record.botId = "other";
      if (condition === "cancelled") receipt.record.cancelRequested = true;
      await expect(f.source.forRun(f.reviewRun.id)).rejects.toThrow("lineage is unavailable");
      expect(f.runner.starts).toBe(1);
    },
  );

  it("propagates unavailable authority as a sanitized refusal without fallback or writes", async () => {
    const f = await finishedDesign();
    f.approvals.forStage = async () => {
      throw new Error("private fake detail");
    };
    await expect(f.source.forRun(f.run.id)).rejects.toThrow("Pi Run lineage is unavailable.");
    expect(f.runner.starts).toBe(1);
    expect(f.approvals.runs.size).toBe(1);
  });
});

describe("Pi lineage uses server provenance rather than nonce/text alone", () => {
  it("does not resolve a user/follow-up Run carrying a copied native nonce", async () => {
    const f = await finishedDesign();
    const original = f.runs.get(f.run.id)!;
    const spoof = f.activeRun("user-copy");
    Object.assign(f.runs.get(spoof.id)!, {
      trigger: "follow_up",
      clientNonce: original.clientNonce,
    });
    expect(await f.source.forRun(spoof.id)).toBeUndefined();
    expect(await f.launch(spoof)).toHaveProperty("error");
    expect(f.runner.starts).toBe(1);
  });
  it("does not give peer text authority when the paired native sender evidence is absent", async () => {
    const f = await deliveredDesign();
    const inbound = f.messages.get(f.runs.get(f.reviewRun.id)!.sourceMessageId)!;
    f.messages.delete(inbound.blocks[0].returnToMessageId);
    await expect(f.source.forRun(f.reviewRun.id)).rejects.toThrow("lineage is unavailable");
    expect(await f.launch(f.reviewRun, "review-bot")).toHaveProperty("error");
    expect(f.runner.starts).toBe(1);
  });
});

describe("Pi child authority retains its original finite owner grant", () => {
  it.each(["deleted-origin", "revoked-root"] as const)(
    "holds queued work when its root is %s",
    async (condition) => {
      const f = lineageFixture();
      const design = await f.register();
      await f.launch();
      if (condition === "deleted-origin") f.runs.delete("origin-run");
      else f.approvals.runs.get("origin-run")!.record.approvalValid = false;
      await f.poll(design.id);
      expect(f.runner.starts).toBe(0);
      expect(f.agents.get(design.id)?.status).toBe("cancelled");
      expect(f.approvals.runs.size).toBe(1);
    },
  );
});

describe("bounded result-return sender ancestry", () => {
  it("reads a review first returned by its launching Run, and preserves the same delivery on wake replay", async () => {
    const f = await deliveredDesign();
    const receipt = supplyApprovedReview(f);
    const review = (await f.launch(f.reviewRun, "review-bot", "review", receipt.record.input)) as {
      id: string;
    };
    await f.poll(review.id);
    f.runner.states.set("work-review", {
      kind: "completed",
      text: "Original review",
      targetHash: receipt.record.targetHash,
    });
    await f.poll(review.id);
    const reviewCaller = f.use(f.reviewRun, "review-bot");
    expect(
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        reviewCaller,
        f.reviewRun,
        { id: "review-bot", name: "Review" },
        { intent: "result", message: await approvedMessage(f, review.id) },
      ),
    ).toHaveProperty("ok", true);
    const returned = [...f.runs.values()].filter((r) => r.trigger === "bot_message").at(-1)!;
    const finalRun = f.activeRun(returned.id);
    const finalCaller = f.use(finalRun);
    expect(
      await executeCloudAgentTool(f.deps, finalCaller, finalRun, "cloud_agent_result", {
        id: review.id,
      }),
    ).toHaveProperty("original");
    const wake = [...f.runs.values()].filter((r) => r.trigger === "cloud_agent").at(-1)!;
    const wakeRun = f.activeRun(wake.id, "review-bot", "review-thread");
    const wakeCaller = f.use(wakeRun, "review-bot");
    expect(
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        wakeCaller,
        wakeRun,
        { id: "review-bot", name: "Review" },
        { intent: "result", message: await approvedMessage(f, review.id) },
      ),
    ).toHaveProperty("ok", true);
    expect([...f.runs.values()].filter((r) => r.trigger === "bot_message")).toHaveLength(2);
    f.use(finalRun);
    expect(
      await executeCloudAgentTool(f.deps, finalCaller, finalRun, "cloud_agent_result", {
        id: review.id,
      }),
    ).toHaveProperty("original");
    expect(await f.source.forRun(finalRun.id)).toMatchObject({ runAccess: "read" });
    expect(f.runner.starts).toBe(2);
  });
  it("rejects a cyclic/read-only result sender without unbounded traversal", async () => {
    const f = await deliveredDesign();
    const receipt = supplyApprovedReview(f);
    const review = (await f.launch(f.reviewRun, "review-bot", "review", receipt.record.input)) as {
      id: string;
    };
    await f.poll(review.id);
    f.runner.states.set("work-review", {
      kind: "completed",
      text: "Original review",
      targetHash: receipt.record.targetHash,
    });
    await f.poll(review.id);
    const reviewCaller = f.use(f.reviewRun, "review-bot");
    await returnFixedPiResult(
      f.deps as ExecutorDeps,
      f.provider,
      reviewCaller,
      f.reviewRun,
      { id: "review-bot", name: "Review" },
      { intent: "result", message: await approvedMessage(f, review.id) },
    );
    const returned = [...f.runs.values()].filter((r) => r.trigger === "bot_message").at(-1)!;
    const finalRun = f.activeRun(returned.id);
    const inbound = f.messages.get(returned.sourceMessageId)!;
    const outbound = f.messages.get(inbound.blocks[0].returnToMessageId)!;
    outbound.runId = finalRun.id;
    await expect(f.source.forRun(finalRun.id)).rejects.toThrow("lineage is unavailable");
    expect(f.runner.starts).toBe(2);
  });
});

function workApprovalFixture(now = () => 1_000) {
  const f = fixture();
  const config = {
    userId: "owner",
    spaceId: "space",
    runnerNamespace: "fake-pi",
    sourcePin: "a".repeat(64),
    modelId: "fake-model",
  };
  const authority = new PrismaPiWorkApprovalSource(f.deps.prisma, f.records, config, now);
  const source = new PiRunApprovalSource(f.deps.prisma, authority, f.records, "fake-pi", now);
  f.deps.piAdmission = new PiNativeAdmission(source, f.registrar, now);
  const proposal = authority.draft({
    originRunId: "origin-run",
    input: "Generalized task",
    design: { botId: "design-bot", threadId: "design-thread" },
    review: { botId: "review-bot", threadId: "review-thread" },
  });
  const owner = async () => ({ userId: "owner", spaceId: "space" });
  return {
    ...f,
    config,
    authority,
    source,
    proposal,
    owner,
    approve: () => authority.registerOwnerWork({ ...proposal, approve: true }, owner),
  };
}

describe("finite two-stage consent reuses private ExternalEffect storage", () => {
  it("derives the review and all child authority from one owner decision, with no fake review receipt injection", async () => {
    const f = workApprovalFixture();
    const approval = await f.approve();
    const designApproval = (await f.authority.forRun("origin-run"))!;
    const designId = piNativeStageId(designApproval.record);
    expect(await f.launch()).toHaveProperty("id", designId);
    await f.poll(designId);
    f.runner.states.set(designApproval.record.handle, {
      kind: "completed",
      text: "Original design",
    });
    await f.poll(designId);
    const wake = [...f.runs.values()].find((r) => r.trigger === "cloud_agent")!;
    const designRun = f.activeRun(wake.id);
    const designCaller = f.use(designRun);
    expect(
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        designCaller,
        designRun,
        { id: "design-bot", name: "Design" },
        { intent: "result", message: await approvedMessage(f, designId) },
      ),
    ).toHaveProperty("ok", true);
    const reviewer = [...f.runs.values()].find((r) => r.trigger === "bot_message")!;
    const reviewRun = f.activeRun(reviewer.id, "review-bot", "review-thread");
    const reviewCaller = f.use(reviewRun, "review-bot");
    const reviewApproval = (await f.source.forRun(reviewRun.id))!;
    expect(reviewApproval).toMatchObject({
      decisionId: approval.decisionId,
      runAccess: "execute",
      record: { stage: "review" },
    });
    expect(reviewApproval.record.input).toContain("Original design");
    expect(reviewApproval.record.targetHash).toBe((await f.records.read(designId))!.original!.hash);
    expect(
      await executeCloudAgentTool(f.deps, reviewCaller, reviewRun, "cloud_agent_result", {
        id: designId,
      }),
    ).toHaveProperty("original");
    const reviewId = piNativeStageId(reviewApproval.record);
    expect(
      await f.launch(reviewRun, "review-bot", "review", reviewApproval.record.input),
    ).toHaveProperty("id", reviewId);
    await f.poll(reviewId);
    f.runner.states.set(reviewApproval.record.handle, {
      kind: "completed",
      text: "Original review",
      targetHash: reviewApproval.record.targetHash,
    });
    await f.poll(reviewId);
    f.use(reviewRun, "review-bot");
    expect(
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        reviewCaller,
        reviewRun,
        { id: "review-bot", name: "Review" },
        { intent: "result", message: await approvedMessage(f, reviewId) },
      ),
    ).toHaveProperty("ok", true);
    const returned = [...f.runs.values()].filter((r) => r.trigger === "bot_message").at(-1)!;
    const finalRun = f.activeRun(returned.id);
    const finalCaller = f.use(finalRun);
    expect(
      await executeCloudAgentTool(f.deps, finalCaller, finalRun, "cloud_agent_result", {
        id: reviewId,
      }),
    ).toMatchObject({ original: { text: "Original review" }, accepted: false });
    expect(f.effects.size).toBe(1);
    expect([...f.effects.values()][0]).toMatchObject({
      kind: PI_WORK_APPROVAL_KIND,
      status: "pi-work-approved",
    });
    expect(f.approvals.runs.size).toBe(0);
    expect(f.approvals.slots.size).toBe(0);
    expect(f.runner.starts).toBe(2);
  });
  it("deduplicates an identical owner decision but refuses changed scope for the same original work", async () => {
    const f = workApprovalFixture();
    await f.approve();
    await f.approve();
    const draft = { ...f.proposal.draft, input: "Different scope" };
    await expect(
      f.authority.registerOwnerWork(
        { approve: true, draft, draftHash: piWorkDraftHash(draft) },
        f.owner,
      ),
    ).rejects.toThrow("readback is required");
    expect(f.effects.size).toBe(1);
    expect(f.runner.starts).toBe(0);
  });
  it.each([
    "no-auth",
    "other-owner",
    "changed-body",
    "changed-hash",
    "expired",
    "wrong-participant",
    "same-bot",
  ] as const)("does not persist approval with %s", async (condition) => {
    const f = workApprovalFixture();
    const draft = structuredClone(f.proposal.draft);
    let auth: () => Promise<{ userId: string; spaceId: string } | undefined> = f.owner;
    let draftHash = f.proposal.draftHash;
    if (condition === "no-auth") auth = async () => undefined;
    if (condition === "other-owner") auth = async () => ({ userId: "other", spaceId: "space" });
    if (condition === "changed-body") draft.input = "Hidden change";
    if (condition === "changed-hash") draftHash = "f".repeat(64);
    if (condition === "expired") {
      draft.issuedAt = 0;
      draft.workUntil = 1;
    }
    if (condition === "wrong-participant") {
      draft.review.threadId = "wrong";
      draftHash = piWorkDraftHash(draft);
    }
    if (condition === "same-bot") {
      draft.review = draft.design;
      draftHash = piWorkDraftHash(draft);
    }
    await expect(
      f.authority.registerOwnerWork({ approve: true, draft, draftHash }, auth),
    ).rejects.toThrow("readback is required");
    expect(f.effects.size).toBe(0);
    expect(f.runner.starts).toBe(0);
  });
  it("does not treat ordinary tool approval as a finite work grant", async () => {
    const f = workApprovalFixture();
    await f.approve();
    const effect = [...f.effects.values()][0]!;
    f.effects.delete(effect.id);
    f.effects.set("ordinary-tool-effect", {
      ...effect,
      id: "ordinary-tool-effect",
      kind: "cloud_agent_launch",
      status: "approved",
    });
    expect(await f.authority.forRun("origin-run")).toBeUndefined();
    expect(await f.launch()).toHaveProperty("error");
    expect(f.runner.starts).toBe(0);
  });
  it("does not derive review from UNKNOWN or altered design originals", async () => {
    for (const condition of ["unknown", "changed-bytes"] as const) {
      const f = workApprovalFixture();
      await f.approve();
      const receipt = (await f.authority.forRun("origin-run"))!;
      const id = piNativeStageId(receipt.record);
      await f.launch();
      await f.poll(id);
      f.runner.states.set(receipt.record.handle, { kind: "completed", text: "Design" });
      await f.poll(id);
      const reviewId = piNativeStageId({ ...receipt.record, stage: "review" });
      await f.records.change(id, (r) => {
        if (condition === "unknown") r.state = "unknown";
        else r.original!.text = "Altered";
      });
      expect(await f.authority.forStage(reviewId)).toBeUndefined();
      expect(f.runner.starts).toBe(1);
    }
  });
  it("revokes the one owner grant without restoring it or starting queued work", async () => {
    const f = workApprovalFixture();
    await f.approve();
    const r = (await f.authority.forRun("origin-run"))!;
    const id = piNativeStageId(r.record);
    await f.launch();
    await f.authority.revoke("origin-run", f.owner);
    await f.authority.revoke("origin-run", f.owner);
    await expect(f.authority.forRun("origin-run")).rejects.toThrow("readback is required");
    await f.poll(id);
    expect(f.runner.starts).toBe(0);
    expect(f.agents.get(id)?.status).toBe("cancelled");
    await expect(f.approve()).rejects.toThrow("readback is required");
  });
  it("does not revoke another owner's record in the same space", async () => {
    const f = workApprovalFixture();
    await f.approve();
    const other = new PrismaPiWorkApprovalSource(
      f.deps.prisma,
      f.records,
      { ...f.config, userId: "other" },
      () => 1_000,
    );
    await expect(
      other.revoke("origin-run", async () => ({ userId: "other", spaceId: "space" })),
    ).rejects.toThrow("readback is required");
    expect([...f.effects.values()][0]!.status).toBe("pi-work-approved");
  });
  it("fails closed after participant thread reassignment or configured source changes", async () => {
    const f = workApprovalFixture();
    await f.approve();
    const changed = new PrismaPiWorkApprovalSource(
      f.deps.prisma,
      f.records,
      { ...f.config, sourcePin: "b".repeat(64) },
      () => 1_000,
    );
    await expect(changed.forRun("origin-run")).rejects.toThrow("readback is required");
    f.bots[1]!.thread.id = "new-thread";
    await expect(f.authority.forRun("origin-run")).rejects.toThrow("readback is required");
  });
});

describe("private work classification and canonical transport regressions", () => {
  it.each(["revoked", "deleted", "ordinary-status"] as const)(
    "does not lose finite work restrictions after its private record is %s",
    async (condition) => {
      const f = workApprovalFixture();
      await f.approve();
      const receipt = (await f.authority.forRun("origin-run"))!;
      const id = piNativeStageId(receipt.record);
      await f.launch();
      if (condition === "revoked") await f.authority.revoke("origin-run", f.owner);
      if (condition === "deleted") f.effects.clear();
      if (condition === "ordinary-status") [...f.effects.values()][0]!.status = "approved";
      const run = { id: "origin-run", threadId: "design-thread" };
      const caller = f.use(run);
      const result = await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        caller,
        run,
        { id: "design-bot", name: "Design" },
        { intent: "result", bot_id: "review-bot", message: "Arbitrary unapproved bytes" },
      );
      expect(result).toHaveProperty("error");
      expect(result).not.toBeUndefined();
      expect([...f.messages.values()].filter((m) => m.clientNonce)).toHaveLength(0);
      await f.poll(id);
      expect(f.runner.starts).toBe(0);
    },
  );
  it.each(["Original design\n", "  Original design  ", "Original design\n\n"])(
    "preserves raw design bytes but produces a launchable review for %j",
    async (text) => {
      const f = workApprovalFixture();
      await f.approve();
      const receipt = (await f.authority.forRun("origin-run"))!;
      const id = piNativeStageId(receipt.record);
      await f.launch();
      await f.poll(id);
      f.runner.states.set(receipt.record.handle, { kind: "completed", text });
      await f.poll(id);
      const reviewId = piNativeStageId({ ...receipt.record, stage: "review" });
      const review = (await f.authority.forStage(reviewId))!;
      const input = JSON.parse(review.record.input.slice(review.record.input.indexOf("\n") + 1));
      expect(input.design.text).toBe(text);
      expect(input.design.sha256).toBe((await f.records.read(id))!.original!.hash);
      expect(review.record.input).toBe(review.record.input.trim());
      const wake = [...f.runs.values()].find((r) => r.trigger === "cloud_agent")!;
      const designRun = f.activeRun(wake.id);
      const caller = f.use(designRun);
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        caller,
        designRun,
        { id: "design-bot", name: "Design" },
        { intent: "result", message: await approvedMessage(f, id) },
      );
      const received = [...f.runs.values()].find((r) => r.trigger === "bot_message")!;
      const run = f.activeRun(received.id, "review-bot", "review-thread");
      expect(await f.launch(run, "review-bot", "review", review.record.input)).toHaveProperty(
        "id",
        reviewId,
      );
    },
  );
  it("canonicalizes before preview and rejects edits to canonical approved bytes", async () => {
    const f = workApprovalFixture();
    const preview = f.authority.draft({
      originRunId: "origin-run",
      input: "  Generalized task\n",
      design: f.proposal.draft.design,
      review: f.proposal.draft.review,
    });
    expect(preview.draft.input).toBe("Generalized task");
    await f.authority.registerOwnerWork({ ...preview, approve: true }, f.owner);
    const draft = { ...preview.draft, input: `${preview.draft.input}\n` };
    await expect(
      f.authority.registerOwnerWork(
        { approve: true, draft, draftHash: preview.draftHash },
        f.owner,
      ),
    ).rejects.toThrow("readback is required");
  });
});

describe("deleted approval does not release its old work identity", () => {
  it("refuses re-registration over an existing stage hold", async () => {
    const f = workApprovalFixture();
    await f.approve();
    await f.launch();
    f.effects.clear();
    await expect(f.approve()).rejects.toThrow("readback is required");
    expect(f.effects.size).toBe(0);
    expect(f.runner.starts).toBe(0);
  });
});

describe("Prisma text storage canonical approval", () => {
  it.each(["Generalized \u0000 task", "Generalized \ud800 task", "Generalized \udc00 task"])(
    "canonicalizes malformed text before preview: %j",
    async (input) => {
      const f = workApprovalFixture();
      const preview = f.authority.draft({
        originRunId: "origin-run",
        input,
        design: f.proposal.draft.design,
        review: f.proposal.draft.review,
      });
      expect(preview.draft.input).toBe("Generalized \ufffd task");
      await f.authority.registerOwnerWork({ ...preview, approve: true }, f.owner);
      expect((await f.authority.forRun("origin-run"))?.record.input).toBe(preview.draft.input);
      expect(() => piWorkDraftHash({ ...preview.draft, input })).toThrow();
    },
  );
  it("preserves valid supplementary Unicode", () => {
    const f = workApprovalFixture();
    expect(
      f.authority.draft({
        originRunId: "origin-run",
        input: "Generalized 😀 task",
        design: f.proposal.draft.design,
        review: f.proposal.draft.review,
      }).draft.input,
    ).toBe("Generalized 😀 task");
  });
  it("rejects malformed opaque identities instead of silently rewriting them", () => {
    const f = workApprovalFixture();
    expect(() =>
      f.authority.draft({
        originRunId: "origin\u0000run",
        input: "Generalized task",
        design: f.proposal.draft.design,
        review: f.proposal.draft.review,
      }),
    ).toThrow();
  });
});

describe("runtime connection vertical design-review-original-owner return", () => {
  it("refuses a provider change even when the SDK model id is unchanged", async () => {
    const f = workApprovalFixture();
    const make = (provider: string, id = "fake-model") =>
      createPiRuntimeConnection({
        prisma: f.deps.prisma,
        store: f.registrar,
        events: f.deps.events,
        scope: { userId: "owner", spaceId: "space" },
        namespace: "fake-pi",
        sourcePin: "a".repeat(64),
        model: { provider, id },
        now: () => 1000,
        resolveModel: async () => ({ provider, id }),
      });
    const first = make("approved-provider");
    const plan = first.authority.draft({
      originRunId: "origin-run",
      input: "Generalized task",
      design: f.proposal.draft.design,
      review: f.proposal.draft.review,
    });
    expect(plan.draft.modelId).toBe("approved-provider/fake-model");
    await first.authority.registerOwnerWork({ ...plan, approve: true }, f.owner);
    await expect(make("other-provider").authority.forRun("origin-run")).rejects.toThrow(
      "readback is required",
    );
    const fields = {
      originRunId: "origin-run",
      input: "Generalized task",
      design: f.proposal.draft.design,
      review: f.proposal.draft.review,
    };
    expect(make("a", "b/c").authority.draft(fields).draft.modelId).not.toBe(
      make("a/b", "c").authority.draft(fields).draft.modelId,
    );
    expect(f.runner.starts).toBe(0);
  });
  it.each([
    "normal",
    "unknown-before-write",
    "commit-response-lost",
    "crash-after-terminal",
    "delayed-before-completion",
  ])("starts both stages and returns originals once: %s", async (scenario) => {
    const f = workApprovalFixture();
    f.runs.get("origin-run")!.trigger = "user";
    f.runs.get("origin-run")!.sourceMessageId = "human-request";
    f.messages.set("human-request", {
      id: "human-request",
      threadId: "design-thread",
      role: "user",
      blocks: [{ kind: "text", text: "Generalized task" }],
    });
    const upstream = vi.fn();
    const connection = createPiRuntimeConnection({
      prisma: f.deps.prisma,
      store: f.registrar,
      events: f.deps.events,
      scope: { userId: "owner", spaceId: "space" },
      namespace: "fake-pi",
      sourcePin: "a".repeat(64),
      model: { provider: "fake", id: "fake-model" },
      now: () => 1000,
      resolveModel: async () => ({ provider: "fake", id: "fake-model" }),
      createRuntime: async (observer: ModelCallObserver) =>
        ({
          describe: () => ({}) as never,
          abort: async () => {},
          async *run(req): AsyncGenerator<AgentRuntimeEvent> {
            await observer.beforeCall({
              runId: req.runId,
              provider: "fake",
              modelId: "fake-model",
              inputTokensEstimate: 1,
              maxOutputTokens: req.model.maxTokens!,
            });
            upstream(req.runId);
            yield {
              type: "text",
              text: req.prompt.includes('"design"') ? "Original review" : "Original design",
            };
            yield { type: "done" };
          },
        }) as AgentRuntime,
    });
    f.deps.cloudAgent = connection.cloudAgent;
    f.deps.piAdmission = connection.piAdmission;
    f.deps.piSupervisorReturn = connection.piSupervisorReturn;
    await connection.authority.registerOwnerWork(
      {
        ...connection.authority.draft({
          originRunId: "origin-run",
          input: "Generalized task",
          design: f.proposal.draft.design,
          review: f.proposal.draft.review,
        }),
        approve: true,
      },
      f.owner,
    );
    const design = (await connection.authority.forRun("origin-run"))!;
    const designId = piNativeStageId(design.record);
    expect(await f.launch()).toHaveProperty("id", designId);
    await f.poll(designId);
    await vi.waitFor(async () => expect((await f.records.read(designId))?.state).toBe("completed"));
    await f.poll(designId);
    const designWake = [...f.runs.values()].find((r) => r.trigger === "cloud_agent")!;
    const designRun = f.activeRun(designWake.id);
    const designCaller = f.use(designRun);
    expect(
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        connection.cloudAgent.provider,
        designCaller,
        designRun,
        { id: "design-bot", name: "Design" },
        { intent: "result", message: await approvedMessage(f, designId) },
      ),
    ).toHaveProperty("ok", true);
    const received = [...f.runs.values()].find((r) => r.trigger === "bot_message")!;
    const reviewRun = f.activeRun(received.id, "review-bot", "review-thread");
    const reviewId = piNativeStageId({ ...design.record, stage: "review" });
    const review = (await connection.authority.forStage(reviewId))!;
    expect(await f.launch(reviewRun, "review-bot", "review", review.record.input)).toHaveProperty(
      "id",
      reviewId,
    );
    await f.poll(reviewId);
    await vi.waitFor(async () => expect((await f.records.read(reviewId))?.state).toBe("completed"));
    if (scenario === "delayed-before-completion")
      f.agents.get(reviewId)!.createdAt = new Date(Date.now() - 300001);
    const publish = connection.piSupervisorReturn.publish.bind(connection.piSupervisorReturn);
    if (scenario === "unknown-before-write" || scenario === "delayed-before-completion")
      vi.spyOn(connection.piSupervisorReturn, "publish").mockResolvedValueOnce({
        status: "unknown",
      });
    if (scenario === "commit-response-lost")
      vi.spyOn(connection.piSupervisorReturn, "publish").mockImplementationOnce(
        async (id, generation) => {
          await publish(id, generation);
          return { status: "unknown" };
        },
      );
    if (scenario === "crash-after-terminal")
      vi.spyOn(connection.piSupervisorReturn, "publish").mockImplementationOnce(async () => {
        throw Error("synthetic process interruption");
      });
    await f.poll(reviewId);
    if (scenario !== "normal") {
      expect(f.agents.get(reviewId)!.status).toBe("finished");
      expect(f.agents.get(reviewId)!.nextPollAt).toBeInstanceOf(Date);
      expect(upstream).toHaveBeenCalledTimes(2);
      await f.poll(reviewId);
    }
    expect(f.agents.get(reviewId)!.nextPollAt).toBeNull();
    const returned = [...f.messages.values()].filter((m) =>
      m.clientNonce?.startsWith("pi-supervisor-result:"),
    );
    expect(returned).toHaveLength(1);
    expect(returned[0]).toMatchObject({ threadId: "design-thread", role: "bot" });
    expect(JSON.parse(returned[0]!.blocks[0].text)).toMatchObject({
      accepted: false,
      original: { design: { text: "Original design" }, review: { text: "Original review" } },
    });
    expect(upstream).toHaveBeenCalledTimes(2);
    const generation = f.agents.get(reviewId)!.generation;
    expect(await connection.piSupervisorReturn.publish(reviewId, generation)).toHaveProperty(
      "status",
      "sent",
    );
    expect(
      [...f.messages.values()].filter((m) => m.clientNonce?.startsWith("pi-supervisor-result:")),
    ).toHaveLength(1);
    expect(upstream).toHaveBeenCalledTimes(2);
    f.runs.get("origin-run")!.trigger = "bot_message";
    expect(await connection.piSupervisorReturn.publish(reviewId, generation)).toHaveProperty(
      "status",
      "unknown",
    );
    expect(upstream).toHaveBeenCalledTimes(2);
  });
});

async function supervisorFlow(bind = true, now = () => 1_000) {
  const f = workApprovalFixture(now);
  f.bots.push({
    id: "supervisor-bot",
    name: "Supervisor",
    thread: { id: "supervisor-thread" },
    disabledBuiltinTools: [],
  });
  f.activeRun("supervisor-request", "supervisor-bot", "supervisor-thread");
  Object.assign(f.runs.get("origin-run")!, {
    trigger: "bot_message",
    sourceMessageId: "initial-inbound",
  });
  f.messages.set("initial-outbound", {
    id: "initial-outbound",
    threadId: "supervisor-thread",
    botId: "supervisor-bot",
    role: "bot",
    runId: "supervisor-request",
    blocks: [
      {
        kind: "bot_message_sent",
        toBotId: "design-bot",
        intent: "request",
        text: "Generalized delegation",
      },
    ],
  });
  f.messages.set("initial-inbound", {
    id: "initial-inbound",
    threadId: "design-thread",
    role: "user",
    runId: "origin-run",
    clientNonce: "bot-message:original-request",
    blocks: [
      {
        kind: "bot_message_received",
        fromBotId: "supervisor-bot",
        intent: "request",
        text: "Generalized delegation",
        returnToMessageId: "initial-outbound",
      },
    ],
  });
  const plan = f.authority.draft({
    originRunId: "origin-run",
    input: "Generalized task",
    design: f.proposal.draft.design,
    review: f.proposal.draft.review,
    ...(bind
      ? {
          supervisor: {
            botId: "supervisor-bot",
            threadId: "supervisor-thread",
            requestMessageId: "initial-outbound",
          },
        }
      : {}),
  });
  await f.authority.registerOwnerWork({ ...plan, approve: true }, f.owner);
  const design = (await f.authority.forRun("origin-run"))!;
  const designId = piNativeStageId(design.record);
  await f.launch();
  await f.poll(designId);
  f.runner.states.set(design.record.handle, { kind: "completed", text: "Design original" });
  await f.poll(designId);
  const wake = [...f.runs.values()].find((r) => r.trigger === "cloud_agent")!;
  const designRun = f.activeRun(wake.id);
  const designCaller = f.use(designRun);
  expect(
    await returnFixedPiResult(
      f.deps as ExecutorDeps,
      f.provider,
      designCaller,
      designRun,
      { id: "design-bot", name: "Design" },
      { intent: "result", message: await approvedMessage(f, designId) },
    ),
  ).toHaveProperty("ok", true);
  const peer = [...f.runs.values()].find(
    (r) => r.trigger === "bot_message" && r.botId === "review-bot",
  )!;
  const reviewRun = f.activeRun(peer.id, "review-bot", "review-thread");
  const reviewId = piNativeStageId({ ...design.record, stage: "review" });
  const review = (await f.authority.forStage(reviewId))!;
  await f.launch(reviewRun, "review-bot", "review", review.record.input);
  await f.poll(reviewId);
  f.runner.states.set(review.record.handle, {
    kind: "completed",
    text: "Review original",
    targetHash: review.record.targetHash,
  });
  await f.poll(reviewId);
  expect(
    await returnFixedPiResult(
      f.deps as ExecutorDeps,
      f.provider,
      f.use(reviewRun, "review-bot"),
      reviewRun,
      { id: "review-bot", name: "Review" },
      { intent: "result", message: await approvedMessage(f, reviewId) },
    ),
  ).toHaveProperty("ok", true);
  const final = [...f.runs.values()]
    .filter((r) => r.trigger === "bot_message" && r.botId === "design-bot")
    .at(-1)!;
  const finalRun = f.activeRun(final.id);
  const caller = f.use(finalRun);
  const send = async (args: Record<string, unknown> = {}) =>
    returnFixedPiResult(
      f.deps as ExecutorDeps,
      f.provider,
      caller,
      finalRun,
      { id: "design-bot", name: "Design" },
      {
        bot_id: "supervisor-bot",
        intent: "result",
        message: piResultReferenceText(
          reviewId,
          "review",
          (await f.records.read(reviewId))!.original!.hash,
        ),
        ...args,
      },
    );
  return { ...f, plan, designId, reviewId, finalRun, caller, send };
}
describe("bounded final supervisor handoff under ordinary message gate", () => {
  it("returns only the approved reference and admits both originals read-only at the original supervisor", async () => {
    const f = await supervisorFlow();
    const result: any = await executeCloudAgentTool(
      f.deps,
      f.caller,
      f.finalRun,
      "cloud_agent_result",
      { id: f.reviewId },
    );
    expect(result.supervisorReturn).toMatchObject({ botId: "supervisor-bot" });
    expect(await f.send({ confirm_name: "Supervisor" })).toHaveProperty("ok", true);
    const received = [...f.runs.values()]
      .filter((r) => r.botId === "supervisor-bot" && r.trigger === "bot_message")
      .at(-1)!;
    const run = f.activeRun(received.id, "supervisor-bot", "supervisor-thread");
    const ctx = f.use(run, "supervisor-bot");
    for (const id of [f.designId, f.reviewId])
      expect(
        await executeCloudAgentTool(f.deps, ctx, run, "cloud_agent_result", { id }),
      ).toMatchObject({
        accepted: false,
        original: { text: id === f.designId ? "Design original" : "Review original" },
      });
    expect((await f.source.forRun(run.id))?.runAccess).toBe("read");
    expect(await f.launch(run, "supervisor-bot", "extra", "unapproved")).toHaveProperty("error");
    expect(
      await returnFixedPiResult(
        f.deps as ExecutorDeps,
        f.provider,
        ctx,
        run,
        { id: "supervisor-bot", name: "Supervisor" },
        { intent: "result", message: "forward elsewhere" },
      ),
    ).toHaveProperty("error");
    expect(f.runner.starts).toBe(2);
  });
  it("does not infer sharing from the original request when it was not in the owner draft", async () => {
    const f = await supervisorFlow(false);
    expect(await f.send()).toHaveProperty("error");
    expect(
      [...f.messages.values()].filter((m) =>
        m.clientNonce?.startsWith("bot-message:pi-supervisor:"),
      ),
    ).toHaveLength(0);
    expect(f.runner.starts).toBe(2);
  });
  it.each(["wrong-target", "wrong-name", "changed-request", "disabled-tool", "lost-member"])(
    "refuses %s before a supervisor send",
    async (kind) => {
      const f = await supervisorFlow();
      let args: Record<string, unknown> = {};
      if (kind === "wrong-target") args = { bot_id: "review-bot" };
      if (kind === "wrong-name") args = { confirm_name: "Review" };
      if (kind === "changed-request")
        f.messages.get("initial-outbound")!.blocks[0].toBotId = "other";
      if (kind === "disabled-tool")
        f.bots.find((b) => b.id === "design-bot")!.disabledBuiltinTools = ["message_bot"];
      if (kind === "lost-member") f.setMember(false);
      expect(await f.send(args)).toHaveProperty("error");
      expect(
        [...f.messages.values()].filter((m) =>
          m.clientNonce?.startsWith("bot-message:pi-supervisor:"),
        ),
      ).toHaveLength(0);
    },
  );
  it("deduplicates final delivery across Run/effect retries without another Pi", async () => {
    const f = await supervisorFlow();
    expect(await f.send()).toHaveProperty("ok", true);
    expect(await f.send()).toHaveProperty("ok", true);
    expect(
      [...f.messages.values()].filter((m) =>
        m.clientNonce?.startsWith("bot-message:pi-supervisor:"),
      ),
    ).toHaveLength(1);
    expect(f.runner.starts).toBe(2);
  });
  it("rejects an arbitrary supervisor injected into a signed owner draft", async () => {
    const f = await supervisorFlow(false);
    const plan = f.authority.draft({
      originRunId: "origin-run",
      input: "Generalized task",
      design: f.proposal.draft.design,
      review: f.proposal.draft.review,
      supervisor: { botId: "review-bot", threadId: "review-thread", requestMessageId: "invented" },
    });
    await expect(
      f.authority.registerOwnerWork({ ...plan, approve: true }, f.owner),
    ).rejects.toThrow();
    expect(f.runner.starts).toBe(2);
  });
});

describe("owner activation via the existing queued Run", () => {
  it("creates one Run, recovers its receipt, and refuses a deleted grant", async () => {
    const f = workApprovalFixture();
    f.runs.get("origin-run")!.status = "completed";
    const receipt = await f.approve();
    const one = await f.authority.activateOwnerWork(receipt.workId, receipt.draftHash, f.owner);
    const two = await f.authority.activateOwnerWork(receipt.workId, receipt.draftHash, f.owner);
    expect(one).toMatchObject({ runState: "queued" });
    if (!("runId" in one) || !one.runId) throw new Error("Missing activation");
    expect(two).toMatchObject({ runId: one.runId });
    expect(f.runs.size).toBe(2);
    expect(await f.source.forRun(one.runId)).toMatchObject({
      runAccess: "execute",
      record: { originRunId: "origin-run" },
    });
    f.effects.clear();
    await expect(f.source.forRun(one.runId)).rejects.toThrow();
  });
  it.each(["running", "waiting"])("does not answer or replace a %s Run", async (status) => {
    const f = workApprovalFixture();
    f.runs.get("origin-run")!.status = status;
    const r = await f.approve();
    expect(await f.authority.activateOwnerWork(r.workId, r.draftHash, f.owner)).toEqual({
      blocked: true,
    });
    expect(f.runs.size).toBe(1);
  });
  it("rejects a cancelled origin", async () => {
    const f = workApprovalFixture();
    f.runs.get("origin-run")!.status = "cancelled";
    const r = await f.approve();
    await expect(f.authority.activateOwnerWork(r.workId, r.draftHash, f.owner)).rejects.toThrow();
    expect(f.runs.size).toBe(1);
  });
  it("reuses a concurrent activation committed after the pre-lock grant snapshot", async () => {
    const f = workApprovalFixture();
    f.runs.get("origin-run")!.status = "completed";
    const receipt = await f.approve();
    const stale = structuredClone(f.effects.get(receipt.workId)!);
    const first = await f.authority.activateOwnerWork(receipt.workId, receipt.draftHash, f.owner);
    const original = f.deps.prisma.externalEffect.findUnique.bind(f.deps.prisma.externalEffect);
    let firstRead = true;
    vi.spyOn(f.deps.prisma.externalEffect, "findUnique").mockImplementation(((args: any) => {
      if (firstRead) {
        firstRead = false;
        return Promise.resolve(stale);
      }
      return original(args);
    }) as typeof f.deps.prisma.externalEffect.findUnique);
    const second = await f.authority.activateOwnerWork(receipt.workId, receipt.draftHash, f.owner);
    expect(second).toMatchObject({ runId: first.runId });
    expect(f.runs.size).toBe(2);
  });
  it("reuses the approved deadline and exact hash in a fresh preview", async () => {
    let clock = 1000;
    const f = workApprovalFixture(() => clock);
    await f.approve();
    clock += 2000;
    expect(
      await f.authority.previewOwnerWork(
        {
          originRunId: "origin-run",
          input: "Generalized task",
          design: f.proposal.draft.design,
          review: f.proposal.draft.review,
        },
        f.owner,
      ),
    ).toEqual(f.proposal);
  });
});
it("refuses supervisor delivery if the last authorization await reaches the work deadline", async () => {
  let clock = 1000;
  const f = await supervisorFlow(true, () => clock);
  const original = f.deps.prisma.run.findFirst.bind(f.deps.prisma.run);
  let checks = 0;
  vi.spyOn(f.deps.prisma.run, "findFirst").mockImplementation((async (arg: any) => {
    const result = await original(arg);
    if (arg.where?.leaseExpiresAt && ++checks === 4) clock = f.plan.draft.workUntil;
    return result;
  }) as typeof f.deps.prisma.run.findFirst);
  expect(await f.send()).toHaveProperty("error");
  expect(checks).toBe(4);
  expect(
    [...f.messages.values()].filter((m) => m.clientNonce?.startsWith("bot-message:pi-supervisor:")),
  ).toHaveLength(0);
  // Serial Map rollback only; real PostgreSQL rollback at this boundary remains untested.
});
