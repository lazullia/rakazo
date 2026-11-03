import type { AgentRuntime, AgentRuntimeEvent, ModelCallObserver } from "@rakazo/adapter-kit";
import { describe, expect, it, vi } from "vitest";
import { PiCloudAgentProvider } from "./pi-cloud-agent.js";
import { binding, context, FakeStageStore } from "./pi-cloud-agent.test-fixtures.js";
import { piNativeStageId } from "./pi-native-admission.js";
import { PiRuntimeRunner } from "./pi-runtime-runner.js";
import { PostgresPiStageStore } from "./pi-stage-postgres.js";

const limits = { maxInferences: 10, maxOutputTokens: 1024, maxDurationMs: 120000 } as const;
function fixture() {
  const records = new FakeStageStore();
  const record = binding({ runtimeProtocol: "pi-runtime/v1" });
  record.handle = piNativeStageId(record);
  records.bind(record.handle, record);
  let now = 1000;
  const started = vi.fn(),
    authorized = vi.fn(async () => true);
  let release: () => void = () => {};
  const gate = new Promise<void>((r) => {
    release = r;
  });
  const createRuntime = async (observer: ModelCallObserver): Promise<AgentRuntime> => ({
    describe: () => ({}) as never,
    abort: async () => {
      release();
    },
    async *run(req): AsyncGenerator<AgentRuntimeEvent> {
      await observer.beforeCall({
        runId: req.runId,
        provider: "fake",
        modelId: "bounded",
        inputTokensEstimate: 1,
        maxOutputTokens: 1024,
      });
      started();
      await gate;
      yield { type: "text", text: "Original result\n" };
      yield { type: "done" };
    },
  });
  const make = () =>
    new PiRuntimeRunner(records, "fake-pi", {
      model: { provider: "fake", id: "bounded" },
      authorized,
      resolveModel: async () => ({ provider: "fake", id: "bounded" }),
      createRuntime,
      now: () => now,
    });
  const runner = make(),
    provider = new PiCloudAgentProvider(records, runner, () => now);
  const launch = () =>
    provider.launch({ idempotencyKey: record.handle, prompt: record.input }, context);
  return {
    records,
    record,
    runner,
    provider,
    make,
    launch,
    started,
    authorized,
    release,
    setNow: (n: number) => {
      now = n;
    },
  };
}
describe("durable runtime acceptance through existing provider", () => {
  it("starts through provider, saves original before observation, and resolves after adapter restart", async () => {
    const f = fixture();
    expect(await f.launch()).toHaveProperty("status", "running");
    await vi.waitFor(() => expect(f.started).toHaveBeenCalledOnce());
    f.release();
    await vi.waitFor(async () =>
      expect((await f.records.read(f.record.handle))?.state).toBe("completed"),
    );
    expect(await f.make().lookup(f.record.handle)).toEqual({
      kind: "completed",
      text: "Original result\n",
    });
    expect(await f.provider.get(f.record.handle, context)).toHaveProperty("status", "finished");
  });
  it("serializes two runner/provider instances onto one accepted slot", async () => {
    const f = fixture();
    const other = new PiCloudAgentProvider(f.records, f.make(), () => 1000);
    await Promise.all([
      f.launch(),
      other.launch({ idempotencyKey: f.record.handle, prompt: f.record.input }, context),
    ]);
    await vi.waitFor(() => expect(f.started).toHaveBeenCalledOnce());
    expect((await f.records.read(f.record.handle))?.runtimeClaim).toBeDefined();
    f.release();
  });
  it("never restarts an accepted operation after the owner disappears", async () => {
    const f = fixture();
    await f.launch();
    await vi.waitFor(() => expect(f.started).toHaveBeenCalledOnce());
    f.setNow(122000);
    const restarted = f.make();
    const provider = new PiCloudAgentProvider(f.records, restarted, () => 122000);
    expect(await provider.get(f.record.handle, context)).toHaveProperty("status", "failed");
    await restarted.startReserved((await f.records.read(f.record.handle))!, limits);
    expect(f.started).toHaveBeenCalledOnce();
    f.release();
    await vi.waitFor(async () =>
      expect((await f.records.read(f.record.handle))?.stopConfirmed).toBe(true),
    );
    expect((await f.records.read(f.record.handle))?.state).toBe("unknown");
  });
  it("cancels an unaccepted reservation and prevents later runtime start", async () => {
    const f = fixture();
    expect(await f.runner.cancel(f.record.handle)).toEqual({
      kind: "cancelled",
      stopConfirmed: true,
    });
    expect(await f.launch()).toHaveProperty("status", "cancelled");
    expect(f.started).not.toHaveBeenCalled();
  });
  it("does not turn a missing legacy acceptance record into stop proof", async () => {
    const f = fixture();
    await f.records.change(f.record.handle, (r) => {
      r.runtimeProtocol = undefined;
      r.dispatched = true;
      r.dispatchGeneration = 1;
    });
    expect(await f.runner.cancel(f.record.handle)).toEqual({ kind: "unknown" });
    expect((await f.records.read(f.record.handle))?.stopConfirmed).toBe(false);
    expect(f.started).not.toHaveBeenCalled();
  });
  it("cancels locally running work only after runtime settles", async () => {
    const f = fixture();
    await f.launch();
    await vi.waitFor(() => expect(f.started).toHaveBeenCalledOnce());
    expect(await f.runner.cancel(f.record.handle)).toEqual({
      kind: "cancelled",
      stopConfirmed: true,
    });
    expect((await f.records.read(f.record.handle))?.state).toBe("cancelled");
  });
  it("leaves a different owner's cancellation unconfirmed until that runtime ends", async () => {
    const f = fixture();
    await f.launch();
    await vi.waitFor(() => expect(f.started).toHaveBeenCalledOnce());
    expect(await f.make().cancel(f.record.handle)).toEqual({
      kind: "cancelled",
      stopConfirmed: false,
    });
    f.release();
    await vi.waitFor(async () =>
      expect(await f.runner.lookup(f.record.handle)).toEqual({
        kind: "cancelled",
        stopConfirmed: true,
      }),
    );
  });
  it("holds a committed-but-lost acceptance without dispatching or replaying", async () => {
    const f = fixture();
    const change = f.records.change.bind(f.records);
    vi.spyOn(f.records, "change").mockImplementation(async (id, fn) => {
      const result = await change(id, fn);
      const r = await f.records.read(id);
      if (r?.runtimeClaim) throw Error("synthetic COMMIT response lost");
      return result;
    });
    await expect(f.launch()).rejects.toThrow("synthetic COMMIT response lost");
    expect(f.started).not.toHaveBeenCalled();
    expect((await f.records.read(f.record.handle))?.runtimeClaim).toBeDefined();
    vi.mocked(f.records.change).mockRestore();
    await f.make().startReserved((await f.records.read(f.record.handle))!, limits);
    expect(f.started).not.toHaveBeenCalled();
  });
  it("never dispatches lookup-only or unauthorized work", async () => {
    const f = fixture();
    expect(await f.runner.lookup(f.record.handle)).toEqual({ kind: "unknown" });
    f.authorized.mockResolvedValue(false);
    await f.launch();
    expect(f.started).not.toHaveBeenCalled();
    expect((await f.records.read(f.record.handle))?.state).toBe("cancelled");
  });
});
describe("Postgres stage adapter runtime claim checks with fake SQL transport", () => {
  function sqlFixture() {
    let record = binding({
      runtimeProtocol: "pi-runtime/v1",
      dispatched: true,
      dispatchGeneration: 1,
    });
    const pool = {
      connect: async () => ({
        release() {},
        query: async (sql: string, values?: unknown[]) => {
          if (sql.startsWith("SELECT w.id"))
            return {
              rows: [
                {
                  id: "row",
                  workId: record.workId,
                  stage: "design",
                  stages: { design: structuredClone(record) },
                },
              ],
            };
          if (sql.startsWith('SELECT stage, "runnerNamespace"'))
            return {
              rows: [
                { stage: "design", runnerNamespace: record.runnerNamespace, handle: record.handle },
              ],
            };
          if (sql.startsWith("UPDATE pi_work_states"))
            record = JSON.parse(values![1] as string).design;
          return { rows: [] };
        },
      }),
    };
    return new PostgresPiStageStore(pool, { userId: "owner", spaceId: "space" });
  }
  it("persists one claim and refuses deletion, reassignment, generation or deadline edits", async () => {
    const store = sqlFixture();
    await store.change("id", (r) => {
      r.runtimeClaim = { owner: "runtime-owner", generation: 1, until: 120000 };
    });
    for (const mutate of [
      (r: any) => {
        r.runtimeClaim = undefined;
      },
      (r: any) => {
        r.runtimeClaim.owner = "other";
      },
      (r: any) => {
        r.runtimeClaim.generation = 2;
      },
      (r: any) => {
        r.runtimeClaim.until++;
      },
    ])
      await expect(store.change("id", mutate)).rejects.toThrow();
    expect((await store.read("id"))?.runtimeClaim?.owner).toBe("runtime-owner");
  });
});
