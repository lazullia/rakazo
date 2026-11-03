import type { CloudAgent, PrismaClient } from "@rakazo/db";
import { describe, expect, it, vi } from "vitest";
import { pollCloudAgent } from "./cloud-agent-poll.js";
import type { CloudAgentDeps } from "./cloud-agent-service.js";
import { cloudAgentBlock, executeCloudAgentTool } from "./cloud-agent-service.js";
import { context, setup } from "./pi-cloud-agent.test-fixtures.js";
import type { PiNativeAdmission } from "./pi-native-admission.js";

vi.mock("@rakazo/db", async (original) => ({
  ...(await original<object>()),
  appendEventInTransaction: async () => ({ seq: 1 }),
}));

/** Deliberately in-memory. No database client is instantiated or server started. */
function nativeFixture() {
  const pi = setup();
  const agent = {
    id: "agent",
    providerKey: "fake-pi",
    spaceId: "space",
    userId: "owner",
    botId: "design-bot",
    threadId: "design-thread",
    messageId: "card",
    status: "running",
    remoteId: null,
    operationKey: "effect",
    launchRequest: { prompt: "Generalized task" },
    launchDispatched: false,
    followup: null,
    followupDispatchedAt: null,
    cancelRequested: false,
    generation: 0,
    wakeGeneration: -1,
    version: 0,
    leaseToken: null,
    leaseExpiresAt: null,
    nextPollAt: new Date(),
    errorCount: 0,
  } as unknown as CloudAgent;
  const card = { id: "card", threadId: "design-thread", blocks: [cloudAgentBlock(agent)] };
  const runs: Array<Record<string, unknown>> = [];
  const tasks: Array<Record<string, unknown>> = [];
  function apply(data: Record<string, unknown>) {
    for (const [key, value] of Object.entries(data)) {
      const target = agent as unknown as Record<string, unknown>;
      target[key] =
        value && typeof value === "object" && "increment" in value
          ? Number(target[key]) + Number(value.increment)
          : value;
    }
  }
  const cloudAgent = {
    findFirst: vi.fn(async () => structuredClone(agent)),
    findUniqueOrThrow: vi.fn(async () => structuredClone(agent)),
    update: vi.fn(async ({ data }: { data: Record<string, unknown> }) => {
      apply(data);
      return structuredClone(agent);
    }),
    updateMany: vi.fn(
      async ({
        where,
        data,
      }: {
        where: { version?: number; leaseToken?: string; OR?: unknown };
        data: Record<string, unknown>;
      }) => {
        if (where.version !== undefined && where.version !== agent.version) return { count: 0 };
        if (where.leaseToken !== undefined && where.leaseToken !== agent.leaseToken)
          return { count: 0 };
        if (where.OR && agent.leaseExpiresAt && agent.leaseExpiresAt > new Date())
          return { count: 0 };
        apply(data);
        return { count: 1 };
      },
    ),
  };
  const tx = {
    cloudAgent,
    message: {
      findFirst: vi.fn(async () => structuredClone(card)),
      update: vi.fn(async ({ data }: { data: { blocks: typeof card.blocks } }) => {
        card.blocks = data.blocks;
      }),
    },
    thread: { updateMany: vi.fn(async () => ({ count: 1 })) },
    spaceMember: { findUnique: vi.fn(async () => ({ id: "membership" })) },
    bot: { findFirst: vi.fn(async () => ({ id: "design-bot" })) },
    task: {
      create: vi.fn(async ({ data }: { data: Record<string, unknown> }) => {
        tasks.push(data);
        return { id: `task-${tasks.length}` };
      }),
    },
    run: {
      create: vi.fn(async ({ data }: { data: Record<string, unknown> }) => {
        runs.push(data);
        return { id: `wake-${runs.length}` };
      }),
    },
  };
  const enqueue = vi.fn(async (_job: unknown) => undefined);
  const deps: CloudAgentDeps = {
    piAdmission: { currentStage: async () => true } as unknown as PiNativeAdmission,
    prisma: {
      ...tx,
      $transaction: async (callback: (client: typeof tx) => unknown) => callback(tx),
    } as unknown as PrismaClient,
    jobs: { enqueue, cancel: async () => undefined, close: async () => undefined },
    events: { notify: vi.fn(async () => undefined) },
    cloudAgent: { key: "fake-pi", spaceId: "space", provider: pi.provider },
  };
  return {
    ...pi,
    agent,
    runs,
    tasks,
    enqueue,
    deps,
    poll: () => pollCloudAgent(deps, { agentId: "agent" }),
  };
}

describe("Pi -> existing CloudAgent poll -> existing queued Bot continuation (offline)", () => {
  it("launches a bound task and queues a single continuation with trusted identity", async () => {
    const f = nativeFixture();
    await f.poll();
    expect(f.runner.starts).toBe(1);
    expect(f.runs).toHaveLength(0);
    f.runner.states.set("work-design", { kind: "completed", text: "Design original" });
    await f.poll();
    await f.poll();
    expect(f.runs).toHaveLength(1);
    expect(f.runs[0]).toMatchObject({
      botId: "design-bot",
      threadId: "design-thread",
      userId: "owner",
      trigger: "cloud_agent",
      clientNonce: "cloud-agent-wake-run:agent:0",
      status: "queued",
    });
    expect(f.runs[0]).not.toHaveProperty("sourceMessageId");
    expect(f.tasks[0]?.prompt).not.toContain("Design original");
    expect(f.tasks[0]?.prompt).toContain("cloud_agent_result");
    expect(await f.provider.readResult("agent", context)).toMatchObject({
      originRunId: "origin-run",
      original: { text: "Design original" },
      accepted: false,
    });
  });
  it("leaves the queued continuation durable when enqueue fails, without reexecuting Pi", async () => {
    const f = nativeFixture();
    await f.poll();
    f.runner.states.set("work-design", { kind: "completed", text: "Original" });
    f.enqueue.mockRejectedValueOnce(new Error("fake queue unavailable"));
    await f.poll();
    await f.poll();
    expect(f.runs).toHaveLength(1);
    expect(f.runs[0]?.status).toBe("queued");
    expect(f.runner.starts).toBe(1);
    expect(f.agent.wakeGeneration).toBe(0);
  });
  it("cancels before first dispatch and also after a native dispatch intent without starting", async () => {
    for (const launchDispatched of [false, true]) {
      const f = nativeFixture();
      f.agent.cancelRequested = true;
      f.agent.launchDispatched = launchDispatched;
      await f.poll();
      expect(f.runner.starts).toBe(0);
      expect(f.agent.status).toBe("cancelled");
      expect(f.runner.cancelCalls).toBe(1);
      expect(f.runs).toHaveLength(1);
    }
  });
  it("reports UNKNOWN without original and never takes native create replay or not_found cancellation", async () => {
    const f = nativeFixture();
    f.runner.loseBeforeStart = true;
    await f.poll();
    await f.poll();
    expect(f.runner.starts).toBe(0);
    expect(f.agent.status).toBe("failed");
    expect(f.runs).toHaveLength(1);
    expect(await f.provider.readResult("agent", context)).toMatchObject({
      executionState: "unknown",
      stopConfirmed: false,
      reason: "execution-unknown",
    });
    await expect(
      f.provider.returnRoute("agent", context, { botId: "review-bot", threadId: "review-thread" }),
    ).rejects.toThrow();
  });
  it("validates the exact approved input before the first native launch", async () => {
    const f = nativeFixture();
    f.agent.launchRequest = { prompt: "Unapproved input" };
    await f.poll();
    expect(f.runner.starts).toBe(0);
    expect(f.agent.status).toBe("failed");
  });
  it("refuses followups at the actual native service boundary", async () => {
    const f = nativeFixture();
    const result = await executeCloudAgentTool(
      f.deps,
      { ...context, botId: "design-bot" },
      { id: "new-run", threadId: "design-thread" },
      "cloud_agent_reply",
      { id: "agent", prompt: "again" },
    );
    expect(result).toEqual({ error: "Cloud agent follow-ups are unavailable." });
    expect(f.agent.generation).toBe(0);
    expect(f.runner.starts).toBe(0);
  });
  it("rejects a native row whose thread disagrees with the trusted binding", async () => {
    const f = nativeFixture();
    f.agent.threadId = "unbound-thread";
    await f.poll();
    expect(f.runner.starts).toBe(0);
    expect(f.agent.status).toBe("failed");
  });
});
