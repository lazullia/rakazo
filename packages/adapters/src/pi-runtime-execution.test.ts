import type {
  AgentRunRequest,
  AgentRuntime,
  AgentRuntimeEvent,
  ModelCallObserver,
} from "@rakazo/adapter-kit";
import { afterEach, describe, expect, it, vi } from "vitest";
import { binding } from "./pi-cloud-agent.test-fixtures.js";
import { PiRuntimeExecution } from "./pi-runtime-execution.js";

function fixture(
  options: { calls?: number; empty?: boolean; missingDone?: boolean; fail?: boolean } = {},
) {
  const record = { ...binding(), dispatched: true, dispatchGeneration: 1 };
  const authorized = vi.fn(async () => true),
    resolveModel = vi.fn(async () => ({
      provider: "fake",
      id: "bounded",
      apiKey: "synthetic-not-a-secret",
    }));
  const requests: AgentRunRequest[] = [];
  const upstream = vi.fn();
  const abort = vi.fn(async () => {});
  const createRuntime = vi.fn(
    async (observer: ModelCallObserver) =>
      ({
        describe: () => ({}) as never,
        abort,
        async *run(request: AgentRunRequest): AsyncGenerator<AgentRuntimeEvent> {
          requests.push(request);
          for (let i = 0; i < (options.calls ?? 1); i++) {
            const reservation = await observer.beforeCall({
              runId: request.runId,
              provider: request.model.provider,
              modelId: request.model.id,
              inputTokensEstimate: 1,
              maxOutputTokens: request.model.maxTokens!,
            });
            upstream();
            await observer.afterCall(reservation, null);
          }
          if (!options.empty) yield { type: "text", text: "Saved original\n" };
          if (options.fail) throw Error("synthetic private exception body");
          if (!options.missingDone) yield { type: "done" };
        },
      }) as AgentRuntime,
  );
  const make = () =>
    new PiRuntimeExecution(record, {
      namespace: record.runnerNamespace,
      model: { provider: "fake", id: "bounded" },
      authorized,
      resolveModel,
      createRuntime,
      now: () => 1000,
    });
  return { record, authorized, resolveModel, createRuntime, requests, upstream, abort, make };
}
afterEach(() => vi.useRealTimers());
describe("bounded Pi runtime execution body", () => {
  it("uses existing run with no tools/history and fixed model/output limit, preserving raw result", async () => {
    const f = fixture();
    const job = f.make();
    expect(await job.execute()).toEqual({ kind: "completed", text: "Saved original\n" });
    expect(f.requests[0]).toMatchObject({
      runId: f.record.handle,
      history: [],
      tools: [],
      model: { provider: "fake", id: "bounded", maxTokens: 1024 },
    });
    expect(f.requests[0]?.fallbackModels).toBeUndefined();
    expect(job.diagnostics()).toMatchObject({ inferences: 1, stopped: true });
  });
  it("starts only once per admitted execution instance", async () => {
    const f = fixture();
    const job = f.make();
    const a = job.execute(),
      b = job.execute();
    expect(a).toBe(b);
    await a;
    await job.execute();
    expect(f.createRuntime).toHaveBeenCalledOnce();
    expect(f.upstream).toHaveBeenCalledOnce();
  });
  it("preserves a stricter existing connection output limit", async () => {
    const f = fixture();
    f.resolveModel.mockResolvedValue({
      provider: "fake",
      id: "bounded",
      apiKey: "synthetic",
      maxTokens: 128,
    } as never);
    expect(await f.make().execute()).toMatchObject({ kind: "completed" });
    expect(f.requests[0]?.model.maxTokens).toBe(128);
  });
  it("refuses a different connection from the configured model profile", async () => {
    const f = fixture();
    f.resolveModel.mockResolvedValue({
      provider: "fake",
      id: "bounded",
      apiKey: "synthetic",
      connectionId: "other-connection",
    } as never);
    expect(await f.make().execute()).toEqual({ kind: "unknown" });
    expect(f.createRuntime).not.toHaveBeenCalled();
  });
  it("takes a private immutable snapshot", async () => {
    const f = fixture();
    const job = f.make();
    f.record.input = "changed after binding";
    await job.execute();
    expect(f.requests[0]?.prompt).not.toBe(f.record.input);
  });
  it("allows ten calls and refuses the eleventh before upstream dispatch", async () => {
    const f = fixture({ calls: 11 });
    const job = f.make();
    expect(await job.execute()).toEqual({ kind: "unknown" });
    expect(f.upstream).toHaveBeenCalledTimes(10);
    expect(job.diagnostics()).toMatchObject({ inferences: 10, reason: "limit", stopped: true });
    expect(JSON.stringify(job.lookup())).not.toContain("Saved original");
  });
  it.each(["empty", "missingDone", "fail"] as const)(
    "fails closed on %s without returning partial text or exception",
    async (key) => {
      const f = fixture({ [key]: true });
      const job = f.make();
      expect(await job.execute()).toEqual({ kind: "unknown" });
      expect(JSON.stringify(job.diagnostics())).not.toContain("exception body");
    },
  );
  it("requires current authority before resolving a model", async () => {
    const f = fixture();
    f.authorized.mockResolvedValue(false);
    expect(await f.make().execute()).toEqual({ kind: "unknown" });
    expect(f.resolveModel).not.toHaveBeenCalled();
  });
  it("rechecks authority before runtime dispatch", async () => {
    const f = fixture();
    f.authorized.mockResolvedValueOnce(true).mockResolvedValueOnce(false);
    expect(await f.make().execute()).toEqual({ kind: "unknown" });
    expect(f.requests).toHaveLength(0);
  });
  it("refuses resolved model changes", async () => {
    const f = fixture();
    f.resolveModel.mockResolvedValue({ provider: "other", id: "bounded", apiKey: "synthetic" });
    expect(await f.make().execute()).toEqual({ kind: "unknown" });
    expect(f.createRuntime).not.toHaveBeenCalled();
  });
  it.each(["unknown", "cancelled", "completed"] as const)(
    "does not execute a saved %s stage",
    async (state) => {
      const f = fixture();
      f.record.state = state;
      expect(await f.make().execute()).toEqual({ kind: "unknown" });
      expect(f.createRuntime).not.toHaveBeenCalled();
    },
  );
  it("checks admission deadline before start, not at each later inference", async () => {
    const f = fixture({ calls: 2 });
    let now = 1000;
    f.record.startUntil = 1500;
    f.authorized.mockImplementation(async () => {
      if (f.upstream.mock.calls.length) now = 2000;
      return true;
    });
    const job = new PiRuntimeExecution(f.record, {
      namespace: f.record.runnerNamespace,
      model: { provider: "fake", id: "bounded" },
      authorized: f.authorized,
      resolveModel: f.resolveModel,
      createRuntime: f.createRuntime,
      now: () => now,
    });
    expect(await job.execute()).toMatchObject({ kind: "completed" });
    expect(f.upstream).toHaveBeenCalledTimes(2);
    const expired = fixture();
    expired.record.startUntil = 1000;
    expect(await expired.make().execute()).toEqual({ kind: "unknown" });
    expect(expired.createRuntime).not.toHaveBeenCalled();
  });
  it("refuses a malformed finite deadline before creating the runtime", async () => {
    const f = fixture();
    f.record.workUntil = Number.NaN;
    expect(await f.make().execute()).toEqual({ kind: "unknown" });
    expect(f.createRuntime).not.toHaveBeenCalled();
  });
  it("carries only the approved review target hash", async () => {
    const f = fixture();
    Object.assign(f.record, { stage: "review", targetHash: "a".repeat(64) });
    expect(await f.make().execute()).toMatchObject({
      kind: "completed",
      targetHash: "a".repeat(64),
    });
  });
  it("tombstones this body when aborted before execution", async () => {
    const f = fixture();
    const job = f.make();
    expect(await job.abort()).toEqual({ kind: "cancelled", stopConfirmed: true });
    expect(await job.execute()).toEqual({ kind: "cancelled", stopConfirmed: true });
    expect(f.resolveModel).not.toHaveBeenCalled();
  });
  it("forwards abort and confirms stop only after runtime iteration settles", async () => {
    const f = fixture();
    let finish: () => void = () => {};
    const blocked = new Promise<void>((resolve) => {
      finish = resolve;
    });
    f.createRuntime.mockImplementation(async () => ({
      describe: () => ({}) as never,
      abort: vi.fn(async () => {
        finish();
      }),
      async *run() {
        await blocked;
        yield { type: "done" } as AgentRuntimeEvent;
      },
    }));
    const job = f.make();
    void job.execute();
    await vi.waitFor(() => expect(f.createRuntime).toHaveBeenCalledOnce());
    expect(await job.abort()).toEqual({ kind: "cancelled", stopConfirmed: true });
    expect(job.diagnostics().stopped).toBe(true);
  });
  it("reports UNKNOWN at two minutes and does not treat deadline as stop confirmation", async () => {
    vi.useFakeTimers();
    const f = fixture();
    let release: () => void = () => {};
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    const abort = vi.fn(async () => {
      await blocked;
    });
    f.createRuntime.mockImplementation(async () => ({
      describe: () => ({}) as never,
      abort,
      async *run() {
        await blocked;
        yield { type: "text", text: "late result" } as AgentRuntimeEvent;
        yield { type: "done" } as AgentRuntimeEvent;
      },
    }));
    const job = f.make();
    const result = job.execute();
    await vi.advanceTimersByTimeAsync(120000);
    expect(await result).toEqual({ kind: "unknown" });
    expect(job.diagnostics().stopped).toBe(false);
    const cancelling = job.abort();
    await vi.advanceTimersByTimeAsync(60000);
    expect(await cancelling).toEqual({ kind: "unknown" });
    release();
    await vi.advanceTimersByTimeAsync(0);
    expect(job.lookup()).toEqual({ kind: "unknown" });
    expect(await job.abort()).toEqual({ kind: "cancelled", stopConfirmed: true });
  });
});
