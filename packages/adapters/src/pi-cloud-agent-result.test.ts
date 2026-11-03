import type { PrismaClient } from "@rakazo/db";
import { describe, expect, it, vi } from "vitest";
import { builtinAgentTools } from "./builtin-tools.js";
import type { CloudAgentDeps } from "./cloud-agent-service.js";
import { executeCloudAgentTool } from "./cloud-agent-service.js";
import { cloudAgentResultSchema } from "./cloud-agent-tools.js";
import { selectCloudAgentTools } from "./cloud-agent-tools-select.js";
import { context, setup } from "./pi-cloud-agent.test-fixtures.js";
import type { PiNativeAdmission } from "./pi-native-admission.js";

async function fixture() {
  const f = setup();
  await f.launch();
  f.runner.states.set("work-design", { kind: "completed", text: "Generalized original" });
  await f.provider.get("agent", context);
  const live = { active: true, thread: true, member: true, bot: true, disabled: false };
  const prisma = {
    run: { findFirst: vi.fn(async () => (live.active ? { id: "continuation" } : null)) },
    thread: { findFirst: vi.fn(async () => (live.thread ? { id: "review-thread" } : null)) },
    bot: {
      findFirst: vi.fn(async () =>
        live.bot
          ? { id: "review-bot", disabledBuiltinTools: live.disabled ? ["cloud_agent_result"] : [] }
          : null,
      ),
    },
    spaceMember: { findUnique: vi.fn(async () => (live.member ? { id: "membership" } : null)) },
  };
  const deps = {
    piAdmission: { authorizeResult: async () => true } as unknown as PiNativeAdmission,
    prisma: prisma as unknown as PrismaClient,
    cloudAgent: { key: "fake-pi", spaceId: "space", provider: f.provider },
    runLease: { runId: "continuation", owner: "worker", fence: 4 },
  } as unknown as CloudAgentDeps;
  const run = { id: "continuation", threadId: "review-thread" };
  const caller = { ...context, botId: "review-bot" };
  return {
    ...f,
    live,
    prisma,
    deps,
    run,
    caller,
    read: (args: Record<string, unknown> = { id: "agent" }) =>
      executeCloudAgentTool(deps, caller, run, "cloud_agent_result", args),
  };
}

describe("server-bound original-result tool", () => {
  it("uses only the authenticated Run and server-fixed recipient/target", async () => {
    const f = await fixture();
    const before = await f.store.read("agent");
    expect(await f.read()).toMatchObject({
      workId: "work",
      stage: "design",
      original: { text: "Generalized original" },
      accepted: false,
    });
    expect(await f.store.read("agent")).toEqual(before);
    expect(f.prisma.run.findFirst).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          id: "continuation",
          botId: "review-bot",
          threadId: "review-thread",
          userId: "owner",
          spaceId: "space",
          status: "running",
          leaseOwner: "worker",
          leaseFence: 4,
        }),
      }),
    );
    expect(f.runner.starts).toBe(1);
    expect(f.runner.cancelCalls).toBe(0);
  });
  it.each(["active", "thread", "member", "bot"] as const)(
    "refuses a currently invalid %s",
    async (field) => {
      const f = await fixture();
      f.live[field] = false;
      expect(await f.read()).toEqual({ error: "Original result is unavailable for this run." });
    },
  );
  it("refuses a tool disabled after this Run started", async () => {
    const f = await fixture();
    f.live.disabled = true;
    expect(await f.read()).toEqual({ error: "Original result is unavailable for this run." });
  });
  it("does not accept a claimed Bot/thread/hash or permission in arguments", async () => {
    const f = await fixture();
    for (const extra of [
      { botId: "design-bot" },
      { threadId: "design-thread" },
      { hash: "claimed" },
      { approvalValid: true },
      { runnerNamespace: "claimed" },
    ]) {
      expect(cloudAgentResultSchema.safeParse({ id: "agent", ...extra }).success).toBe(false);
      expect(await f.read({ id: "agent", ...extra })).toEqual({
        error: "Original result is unavailable for this run.",
      });
    }
  });
  it("refuses the wrong fixed recipient, grant revocation and changed original hash", async () => {
    const f = await fixture();
    expect(
      await executeCloudAgentTool(
        f.deps,
        { ...f.caller, botId: "other" },
        f.run,
        "cloud_agent_result",
        { id: "agent" },
      ),
    ).toEqual({ error: "Original result is unavailable for this run." });
    await f.store.change("agent", (record) => {
      record.approvalValid = false;
    });
    expect(await f.read()).toEqual({ error: "Original result is unavailable for this run." });
    await f.store.change("agent", (record) => {
      record.approvalValid = true;
      record.original!.text = "changed bytes";
    });
    expect(await f.read()).toEqual({ error: "Original result is unavailable for this run." });
  });
  it("does not emit bytes if membership changes during the provider read", async () => {
    const f = await fixture();
    const read = f.provider.readForRun.bind(f.provider);
    vi.spyOn(f.provider, "readForRun").mockImplementation(async (...args) => {
      const result = await read(...args);
      f.live.member = false;
      return result;
    });
    expect(await f.read()).toEqual({ error: "Original result is unavailable for this run." });
  });
  it("requires this Run's server lease and offers the tool only with original-reader capability", async () => {
    const f = await fixture();
    f.deps.runLease = undefined;
    expect(await f.read()).toEqual({ error: "Original result is unavailable for this run." });
    expect(
      selectCloudAgentTools(builtinAgentTools, true).some(
        (tool) => tool.name === "cloud_agent_result",
      ),
    ).toBe(false);
    expect(
      selectCloudAgentTools(builtinAgentTools, true, true).some(
        (tool) => tool.name === "cloud_agent_result",
      ),
    ).toBe(true);
  });
});
