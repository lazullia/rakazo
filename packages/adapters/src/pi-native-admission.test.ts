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
import { PiNativeAdmission, piNativeStageId, registerPiOwnerStage } from "./pi-native-admission.js";

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
  let member = true;
  const activeRun = (id: string, botId = "design-bot", threadId = "design-thread") => {
    runs.set(id, {
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
    thread: {
      update: async ({ where }: Row) => ({ id: where.id }),
      updateMany: async () => ({ count: 1 }),
      findFirst: async ({ where }: Row) =>
        bots.some((b) => b.thread.id === where.id && (!where.botId || b.id === where.botId))
          ? { id: where.id }
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
      const next = tail.then(() => fn(tx));
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
    await expect(f.launch()).rejects.toThrow("not authorized");
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
