import { createHash } from "node:crypto";
import {
  applyJudgeDecision,
  planActionGate,
  resolveActionApprovalDetail,
  toolRequiresApproval,
} from "@rakazo/core";
import { describe, expect, it } from "vitest";
import { PiCloudAgentProvider } from "./pi-cloud-agent.js";

import { binding, context, setup } from "./pi-cloud-agent.test-fixtures.js";

describe("Pi provider against a fake runner and trusted store", () => {
  it("launches once across aliases, concurrency and provider recreation", async () => {
    const { store, runner, launch } = setup();
    store.bind("new-operation", binding());
    await Promise.all([launch(), launch(), launch("new-operation")]);
    await new PiCloudAgentProvider(store, runner, () => 1).launch(
      { idempotencyKey: "agent", prompt: "Generalized task" },
      context,
    );
    expect(runner.starts).toBe(1);
    expect(runner.limits).toEqual({
      maxInferences: 10,
      maxOutputTokens: 1024,
      maxDurationMs: 120_000,
    });
    expect(() => store.bind("other-run", binding({ botId: "other-bot" }))).toThrow(
      "stage already bound",
    );
  });
  it("rejects an unbound task, caller identity changes and prompt/egress changes", async () => {
    const { provider, runner } = setup();
    for (const ctx of [
      { ...context, userId: "other" },
      { ...context, botId: "other" },
      { ...context, spaceId: "other" },
    ]) {
      await expect(
        provider.launch({ idempotencyKey: "agent", prompt: "Generalized task" }, ctx),
      ).rejects.toThrow();
    }
    for (const request of [
      { idempotencyKey: "unbound", prompt: "Generalized task" },
      { idempotencyKey: "agent", prompt: "Other task" },
      { idempotencyKey: "agent", prompt: "Generalized task", openPr: true },
    ]) {
      await expect(provider.launch(request, context)).rejects.toThrow();
    }
    expect(runner.starts).toBe(0);
  });
  it("never creates from a query or from missing dispatch reconciliation", async () => {
    const { provider, runner, launch } = setup();
    runner.loseBeforeStart = true;
    expect((await launch()).status).toBe("failed");
    runner.loseBeforeStart = false;
    expect((await launch()).status).toBe("failed");
    expect((await provider.get("agent", context)).status).toBe("failed");
    expect(runner.starts).toBe(0);
    expect(await provider.readResult("agent", context)).toMatchObject({
      executionState: "unknown",
      reason: "execution-unknown",
      stopConfirmed: false,
      accepted: false,
    });
  });
  it("does not restart after a lost response or automatically release an UNKNOWN hold", async () => {
    const { provider, runner, launch } = setup();
    runner.loseStartReply = true;
    await launch();
    await launch();
    runner.states.set("work-design", { kind: "completed", text: "Design original" });
    expect((await provider.get("agent", context)).status).toBe("failed");
    expect(await provider.readResult("agent", context)).toMatchObject({
      executionState: "unknown",
      original: { text: "Design original" },
      stopConfirmed: true,
      accepted: false,
    });
    await expect(
      provider.returnRoute("agent", context, { botId: "review-bot", threadId: "review-thread" }),
    ).rejects.toThrow();
    expect(runner.starts).toBe(1);
  });
  it.each(["cancel", "expired", "revoked", "sharing-denied"])(
    "does not start a %s reservation",
    async (kind) => {
      const { provider, runner, launch } = setup(
        binding({
          cancelRequested: kind === "cancel",
          startUntil: kind === "expired" ? 0 : 600_000,
          approvalValid: kind !== "revoked",
          sharingAllowed: kind !== "sharing-denied",
        }),
      );
      expect((await launch()).status).toBe("cancelled");
      expect((await provider.get("agent", context)).status).toBe("cancelled");
      expect(runner.starts).toBe(0);
    },
  );
  it("serializes cancel before a paused fake start and never starts after cancellation completion", async () => {
    const { provider, runner, launch } = setup();
    let release!: () => void;
    runner.beforeStart = () =>
      new Promise<void>((done) => {
        release = done;
      });
    const pending = launch();
    await new Promise((done) => setImmediate(done));
    expect((await provider.cancel("agent", context)).status).toBe("cancelled");
    release();
    await pending;
    expect((await launch()).status).toBe("cancelled");
    expect(runner.starts).toBe(0);
  });
  it("rechecks the start deadline at the fake runner serialization point", async () => {
    const { runner, launch } = setup();
    runner.beforeStart = async () => {
      runner.now = 600_001;
    };
    expect((await launch()).status).toBe("cancelled");
    expect(runner.starts).toBe(0);
  });
  it("does not call an unconfirmed stop cancelled; timeout/not_found remain unknown", async () => {
    const { provider, runner, launch } = setup();
    await launch();
    runner.cancelPending = true;
    expect((await provider.cancel("agent", context)).status).toBe("failed");
    runner.states.delete("work-design");
    expect((await provider.get("agent", context)).status).toBe("failed");
    expect(await provider.readResult("agent", context)).toMatchObject({
      stopConfirmed: false,
      executionState: "unknown",
    });
    await expect(
      provider.returnRoute("agent", context, { botId: "review-bot", threadId: "review-thread" }),
    ).rejects.toThrow();
    expect(runner.starts).toBe(1);
  });
  it("reads an immutable original, checks fixed return and keeps acceptance separate", async () => {
    const { provider, runner, launch } = setup();
    await launch();
    runner.states.set("work-design", { kind: "completed", text: "Design original" });
    expect((await provider.get("agent", context)).status).toBe("finished");
    const result = await provider.readResult("agent", context);
    expect(result.original?.hash).toBe(
      createHash("sha256").update("Design original").digest("hex"),
    );
    expect(result.accepted).toBe(false);
    expect(
      await provider.returnRoute("agent", context, {
        botId: "review-bot",
        threadId: "review-thread",
      }),
    ).toEqual({ botId: "review-bot", threadId: "review-thread" });
    await expect(
      provider.returnRoute("agent", context, { botId: "review-bot", threadId: "new-thread" }),
    ).rejects.toThrow();
    runner.states.clear();
    expect((await provider.get("agent", context)).status).toBe("finished");
  });
  it("does not regress a saved completion on an older in-flight running read", async () => {
    const { provider, runner, launch } = setup();
    await launch();
    let release!: () => void;
    const lookup = runner.lookup.bind(runner);
    runner.lookup = async () => {
      await new Promise<void>((done) => {
        release = done;
      });
      return { kind: "running" };
    };
    const oldRead = provider.get("agent", context);
    await new Promise((done) => setImmediate(done));
    runner.lookup = lookup;
    runner.states.set("work-design", { kind: "completed", text: "Original" });
    expect((await provider.get("agent", context)).status).toBe("finished");
    release();
    expect((await oldRead).status).toBe("finished");
  });
  it("holds a review on a wrong design target and rejects followups", async () => {
    const { provider, runner } = setup(binding({ stage: "review", targetHash: "design-hash" }));
    runner.states.set("work-design", {
      kind: "completed",
      text: "Review",
      targetHash: "wrong-hash",
    });
    expect((await provider.get("agent", context)).status).toBe("failed");
    expect(await provider.readResult("agent", context)).toMatchObject({
      reason: "target-mismatch",
    });
    expect(provider.describe().capabilities.reply).toBe(false);
    await expect(provider.reply("agent", { prompt: "again" }, context)).rejects.toThrow();
    expect(runner.starts).toBe(0);
  });
  it("binds review to the completed design bytes and returns each result separately", async () => {
    const { store, provider, runner, launch } = setup();
    await launch();
    runner.states.set("work-design", { kind: "completed", text: "Generalized design" });
    await provider.get("agent", context);
    const original = (await provider.readResult("agent", context)).original!;
    const review = binding({
      stage: "review",
      handle: "work-review",
      botId: "review-bot",
      threadId: "review-thread",
      input: original.text,
      targetHash: original.hash,
      returnTo: { botId: "design-bot", threadId: "design-thread" },
    });
    store.bind("review-agent", review);
    const reviewer = { ...context, botId: "review-bot" };
    expect(
      (await provider.launch({ idempotencyKey: "review-agent", prompt: original.text }, reviewer))
        .status,
    ).toBe("running");
    runner.states.set("work-review", {
      kind: "completed",
      text: "Review original",
      targetHash: original.hash,
    });
    expect((await provider.get("review-agent", reviewer)).status).toBe("finished");
    expect(await provider.readResult("review-agent", reviewer)).toMatchObject({
      stage: "review",
      accepted: false,
      original: { text: "Review original", targetHash: original.hash },
    });
    expect(
      await provider.returnRoute("review-agent", reviewer, {
        botId: "design-bot",
        threadId: "design-thread",
      }),
    ).toMatchObject({ threadId: "design-thread" });
    expect(runner.starts).toBe(2);
    await store.change("agent", (record) => {
      record.cancelRequested = true;
    });
    await expect(
      provider.returnRoute("review-agent", reviewer, {
        botId: "design-bot",
        threadId: "design-thread",
      }),
    ).rejects.toThrow();
  });
  it("will not start a pre-bound review before design is completed", async () => {
    const { store, provider, runner } = setup();
    store.bind(
      "review-agent",
      binding({
        stage: "review",
        handle: "work-review",
        botId: "review-bot",
        input: "Review",
        targetHash: "not-a-completed-design",
      }),
    );
    expect(
      (
        await provider.launch(
          { idempotencyKey: "review-agent", prompt: "Review" },
          { ...context, botId: "review-bot" },
        )
      ).status,
    ).toBe("cancelled");
    expect(runner.starts).toBe(0);
  });
  it("leaves normal approval policy intact: allow needs no extra card, ask/deny do not inherit", () => {
    const gate = (effect: "always_allow" | "require_approval" | "deny") =>
      planActionGate({
        resolved: resolveActionApprovalDetail({
          toolName: "cloud_agent_launch",
          rules: [{ effect, matchKind: "tool", matchValue: "cloud_agent_launch" }],
        }),
        mode: "ask",
        consequential: toolRequiresApproval("cloud_agent_launch", false),
        checkerConfigured: false,
      });
    expect(gate("always_allow")).toBe("allow");
    expect(gate("require_approval")).toBe("ask");
    expect(gate("deny")).toBe("deny");
    expect(applyJudgeDecision({ decision: "pass", consequential: true })).toBe("allow");
  });
});
