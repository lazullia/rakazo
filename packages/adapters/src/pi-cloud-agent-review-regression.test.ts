import { describe, expect, it } from "vitest";
import { PiCloudAgentProvider } from "./pi-cloud-agent.js";
import { binding, context, setup } from "./pi-cloud-agent.test-fixtures.js";

describe("independent review reproductions", () => {
  it("keeps a live dispatch running when a parallel launch cannot yet find it", async () => {
    const { provider, runner, launch } = setup();
    let release!: () => void;
    runner.beforeStart = () =>
      new Promise<void>((done) => {
        release = done;
      });
    const first = launch();
    await new Promise((done) => setImmediate(done));
    const parallel = await launch();
    expect(parallel.status).toBe("running");
    release();
    await first;
    runner.states.set("work-design", { kind: "completed", text: "Original" });
    expect((await provider.get("agent", context)).status).toBe("finished");
    expect(runner.starts).toBe(1);
  });
  it("finishes and returns a started task after its start admission deadline", async () => {
    const { store, runner } = setup(binding({ startUntil: 10 }));
    let now = 1;
    const provider = new PiCloudAgentProvider(store, runner, () => now);
    await provider.launch({ idempotencyKey: "agent", prompt: "Generalized task" }, context);
    now = 11;
    runner.states.set("work-design", { kind: "completed", text: "Original" });
    expect((await provider.reconcile("agent", context, false)).status).toBe("finished");
    expect(
      await provider.returnRoute("agent", context, {
        botId: "review-bot",
        threadId: "review-thread",
      }),
    ).toEqual({ botId: "review-bot", threadId: "review-thread" });
    expect(runner.cancelCalls).toBe(0);
  });
  it("lets the fixed receiving Bot read the original without impersonating its executor", async () => {
    const { provider, runner, launch } = setup();
    await launch();
    runner.states.set("work-design", { kind: "completed", text: "Original" });
    await provider.get("agent", context);
    const original = (await provider.readResult("agent", context)).original!;
    const result = await provider.readHandoff(
      "agent",
      { ...context, botId: "review-bot", threadId: "review-thread" },
      {
        workId: "work",
        stage: "design",
        originalHash: original.hash,
      },
    );
    expect(result.original?.text).toBe("Original");
  });
});

describe("dispatch ownership and genuine uncertainty", () => {
  it("shares the pending dispatch across provider instances without a missing lookup", async () => {
    const { store, runner, launch } = setup();
    let release!: () => void;
    runner.beforeStart = () =>
      new Promise<void>((done) => {
        release = done;
      });
    const first = launch();
    await new Promise((done) => setImmediate(done));
    const other = new PiCloudAgentProvider(store, runner, () => 1);
    expect((await other.reconcile("agent", context, false)).status).toBe("running");
    expect((await other.get("agent", context)).status).toBe("running");
    expect(runner.lookups).toBe(0);
    release();
    await first;
    runner.states.set("work-design", { kind: "completed", text: "Original" });
    expect((await other.get("agent", context)).status).toBe("finished");
    expect(runner.starts).toBe(1);
  });
  it("holds a dispatch whose owner lease expired even when a late original arrives", async () => {
    const { store, runner } = setup();
    let now = 1;
    const provider = new PiCloudAgentProvider(store, runner, () => now);
    let release!: () => void;
    runner.beforeStart = () =>
      new Promise<void>((done) => {
        release = done;
      });
    const first = provider.launch({ idempotencyKey: "agent", prompt: "Generalized task" }, context);
    await new Promise((done) => setImmediate(done));
    now = 60_002;
    expect((await provider.reconcile("agent", context, false)).status).toBe("failed");
    expect(await provider.readResult("agent", context)).toMatchObject({
      executionState: "unknown",
      stopConfirmed: false,
    });
    release();
    expect((await first).status).toBe("failed");
    runner.states.set("work-design", { kind: "completed", text: "Late original" });
    expect((await provider.get("agent", context)).status).toBe("failed");
    expect(await provider.readResult("agent", context)).toMatchObject({
      executionState: "unknown",
      stopConfirmed: true,
      original: { text: "Late original" },
    });
    expect(runner.starts).toBe(1);
  });
  it("ignores the response of an older fenced dispatch generation", async () => {
    const { store, runner, provider, launch } = setup();
    let release!: () => void;
    runner.beforeStart = () =>
      new Promise<void>((done) => {
        release = done;
      });
    const first = launch();
    await new Promise((done) => setImmediate(done));
    await store.change("agent", (record) => {
      record.dispatchGeneration += 1;
      record.dispatchClaim = {
        owner: "new-owner",
        generation: record.dispatchGeneration,
        until: 60_001,
      };
    });
    release();
    expect((await first).status).toBe("running");
    expect((await provider.readResult("agent", context)).original).toBeUndefined();
    expect((await store.read("agent"))?.dispatchClaim?.owner).toBe("new-owner");
    expect(runner.starts).toBe(0);
  });
});

describe("separate admission, work and stop-reconciliation deadlines", () => {
  it("allows review and result return after the design admission window closed", async () => {
    const { store, runner } = setup(
      binding({ startUntil: 2, workUntil: 1000, stopReconcileUntil: 2000 }),
    );
    let now = 1;
    const provider = new PiCloudAgentProvider(store, runner, () => now);
    await provider.launch({ idempotencyKey: "agent", prompt: "Generalized task" }, context);
    now = 3;
    runner.now = now;
    runner.states.set("work-design", { kind: "completed", text: "Design" });
    await provider.get("agent", context);
    const original = (await provider.readResult("agent", context)).original!;
    store.bind(
      "review",
      binding({
        handle: "review-handle",
        stage: "review",
        input: original.text,
        targetHash: original.hash,
        botId: "review-bot",
        threadId: "review-thread",
        startUntil: 10,
        workUntil: 1000,
        stopReconcileUntil: 2000,
        returnTo: { botId: "design-bot", threadId: "design-thread" },
      }),
    );
    const reviewer = { ...context, botId: "review-bot" };
    expect(
      (await provider.launch({ idempotencyKey: "review", prompt: original.text }, reviewer)).status,
    ).toBe("running");
    now = 11;
    runner.now = now;
    runner.states.set("review-handle", {
      kind: "completed",
      text: "Review",
      targetHash: original.hash,
    });
    expect((await provider.reconcile("review", reviewer, false)).status).toBe("finished");
    expect(
      await provider.returnRoute("review", reviewer, {
        botId: "design-bot",
        threadId: "design-thread",
      }),
    ).toEqual({ botId: "design-bot", threadId: "design-thread" });
    const review = (await provider.readResult("review", reviewer)).original!;
    expect(
      await provider.readHandoff(
        "review",
        { ...context, threadId: "design-thread" },
        { workId: "work", stage: "review", originalHash: review.hash, targetHash: original.hash },
      ),
    ).toMatchObject({ original: { text: "Review" }, accepted: false });
    expect(runner.starts).toBe(2);
    expect(runner.cancelCalls).toBe(0);
  });
  it("closes work at its own deadline without claiming an unconfirmed stop", async () => {
    const { store, runner } = setup(
      binding({ startUntil: 5, workUntil: 10, stopReconcileUntil: 20 }),
    );
    let now = 1;
    const provider = new PiCloudAgentProvider(store, runner, () => now);
    await provider.launch({ idempotencyKey: "agent", prompt: "Generalized task" }, context);
    now = 11;
    runner.cancelPending = true;
    expect((await provider.reconcile("agent", context, false)).status).toBe("failed");
    expect(await provider.readResult("agent", context)).toMatchObject({
      executionState: "unknown",
      stopConfirmed: false,
      reconciliationExpired: false,
    });
    const calls = { cancel: runner.cancelCalls, lookup: runner.lookups };
    now = 21;
    expect((await provider.reconcile("agent", context, false)).status).toBe("failed");
    expect((await provider.get("agent", context)).status).toBe("failed");
    expect(await provider.readResult("agent", context)).toMatchObject({
      stopConfirmed: false,
      reconciliationExpired: true,
    });
    expect({ cancel: runner.cancelCalls, lookup: runner.lookups }).toEqual(calls);
    expect(runner.starts).toBe(1);
  });
});

describe("fixed recipient read-only authorization", () => {
  it("rejects wrong callers, threads, targets and revoked sharing without any runner mutation", async () => {
    const { store, runner, provider, launch } = setup();
    await launch();
    runner.states.set("work-design", { kind: "completed", text: "Original" });
    await provider.get("agent", context);
    const original = (await provider.readResult("agent", context)).original!;
    const recipient = { ...context, botId: "review-bot", threadId: "review-thread" };
    const target = { workId: "work", stage: "design" as const, originalHash: original.hash };
    const before = await store.read("agent");
    await provider.readHandoff("agent", recipient, target);
    expect(await store.read("agent")).toEqual(before);
    for (const caller of [
      { ...recipient, userId: "other" },
      { ...recipient, spaceId: "other" },
      { ...recipient, botId: "other" },
      { ...recipient, threadId: "other" },
    ]) {
      await expect(provider.readHandoff("agent", caller, target)).rejects.toThrow();
    }
    for (const wrongTarget of [
      { ...target, workId: "other" },
      { ...target, stage: "review" as const },
      { ...target, originalHash: "wrong" },
      { ...target, targetHash: "unexpected" },
    ]) {
      await expect(provider.readHandoff("agent", recipient, wrongTarget)).rejects.toThrow();
    }
    await expect(provider.readResult("agent", recipient)).rejects.toThrow();
    await expect(
      provider.launch({ idempotencyKey: "agent", prompt: "Generalized task" }, recipient),
    ).rejects.toThrow();
    await expect(provider.get("agent", recipient)).rejects.toThrow();
    await expect(provider.cancel("agent", recipient)).rejects.toThrow();
    await store.change("agent", (record) => {
      record.sharingAllowed = false;
    });
    await expect(provider.readHandoff("agent", recipient, target)).rejects.toThrow();
    expect(runner.starts).toBe(1);
    expect(runner.cancelCalls).toBe(0);
  });
  it("does not share a late original still held as UNKNOWN", async () => {
    const { provider, runner, launch } = setup();
    runner.loseStartReply = true;
    await launch();
    runner.states.set("work-design", { kind: "completed", text: "Late" });
    await provider.get("agent", context);
    const original = (await provider.readResult("agent", context)).original!;
    await expect(
      provider.readHandoff(
        "agent",
        { ...context, botId: "review-bot", threadId: "review-thread" },
        { workId: "work", stage: "design", originalHash: original.hash },
      ),
    ).rejects.toThrow();
    expect(runner.starts).toBe(1);
  });
});
