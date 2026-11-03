import type { AdapterContext } from "@rakazo/adapter-kit";
import type {
  PiObservation,
  PiStageRecord,
  PiStageRunner,
  PiStageStore,
} from "./pi-cloud-agent.js";
import { PiCloudAgentProvider } from "./pi-cloud-agent.js";

export const context: AdapterContext = {
  operationId: "effect-design",
  traceId: "trace",
  signal: new AbortController().signal,
  userId: "owner",
  spaceId: "space",
  botId: "design-bot",
};

/** Fake durable boundary shared across provider instances, not a production DB adapter. */
export class FakeStageStore implements PiStageStore {
  readonly records = new Map<string, PiStageRecord>();
  readonly aliases = new Map<string, string>();
  bind(agentId: string, record: PiStageRecord) {
    const key = `${record.ownerId}:${record.spaceId}:${record.workId}:${record.stage}`;
    const existing = this.records.get(key);
    if (existing && JSON.stringify(existing) !== JSON.stringify(record))
      throw new Error("stage already bound");
    this.records.set(key, existing ?? structuredClone(record));
    this.aliases.set(agentId, key);
  }
  async read(id: string) {
    return structuredClone(this.records.get(this.aliases.get(id) ?? ""));
  }
  async change<T>(
    id: string,
    update: (record: PiStageRecord, design?: PiStageRecord) => T,
  ): Promise<T> {
    const record = this.records.get(this.aliases.get(id) ?? "");
    if (!record) throw new Error("unbound");
    // Synchronous transaction callback serializes this fake store only.
    const design = this.records.get(`${record.ownerId}:${record.spaceId}:${record.workId}:design`);
    return update(record, design);
  }
}

export function binding(overrides: Partial<PiStageRecord> = {}): PiStageRecord {
  return {
    handle: "work-design",
    runnerNamespace: "fake-pi",
    workId: "work",
    stage: "design",
    ownerId: "owner",
    spaceId: "space",
    botId: "design-bot",
    threadId: "design-thread",
    originRunId: "origin-run",
    approvalRef: "finite-grant-v1",
    approvalValid: true,
    sharingAllowed: true,
    startUntil: 600_000,
    workUntil: 600_000,
    stopReconcileUntil: 900_000,
    dispatchGeneration: 0,
    input: "Generalized task",
    returnTo: { botId: "review-bot", threadId: "review-thread" },
    dispatched: false,
    cancelRequested: false,
    state: "reserved",
    stopConfirmed: false,
    ...overrides,
  };
}

export class FakePiRunner implements PiStageRunner {
  readonly namespace = "fake-pi";
  readonly states = new Map<string, PiObservation>();
  now = 1;
  starts = 0;
  lookups = 0;
  cancelCalls = 0;
  loseStartReply = false;
  loseBeforeStart = false;
  cancelPending = false;
  beforeStart?: () => Promise<void>;
  limits?: Parameters<PiStageRunner["startReserved"]>[1];
  constructor(private readonly store: FakeStageStore) {}
  async startReserved(
    record: PiStageRecord,
    limits: Parameters<PiStageRunner["startReserved"]>[1],
  ) {
    this.limits = limits;
    await this.beforeStart?.();
    const current = [...this.store.records.values()].find((item) => item.handle === record.handle)!;
    if (
      record.dispatchClaim?.owner !== current.dispatchClaim?.owner ||
      record.dispatchClaim?.generation !== current.dispatchClaim?.generation
    )
      return { kind: "unknown" as const };
    const design = [...this.store.records.values()].find(
      (item) =>
        item.workId === record.workId &&
        item.ownerId === record.ownerId &&
        item.spaceId === record.spaceId &&
        item.stage === "design",
    );
    const blockedReview =
      record.stage === "review" &&
      (design?.state !== "completed" ||
        !design.approvalValid ||
        !design.sharingAllowed ||
        this.now >= design.workUntil ||
        design.cancelRequested ||
        design.original?.hash !== record.targetHash);
    if (
      current.cancelRequested ||
      !current.approvalValid ||
      !current.sharingAllowed ||
      this.now >= current.startUntil ||
      this.now >= current.workUntil ||
      blockedReview ||
      this.states.get(record.handle)?.kind === "cancelled"
    ) {
      this.states.set(record.handle, { kind: "cancelled", stopConfirmed: true });
      return this.states.get(record.handle)!;
    }
    if (this.loseBeforeStart) throw new Error("fake transport error; must not be exposed");
    if (!this.states.has(record.handle)) {
      this.starts++;
      this.states.set(record.handle, { kind: "running" });
    }
    if (this.loseStartReply) throw new Error("fake transport error; must not be exposed");
    return this.states.get(record.handle)!;
  }
  async lookup(handle: string) {
    this.lookups++;
    return this.states.get(handle) ?? { kind: "missing" as const };
  }
  async cancel(handle: string): Promise<PiObservation> {
    this.cancelCalls++;
    if (this.cancelPending) return { kind: "cancelled", stopConfirmed: false };
    this.states.set(handle, { kind: "cancelled", stopConfirmed: true });
    return this.states.get(handle)!;
  }
}

export function setup(record = binding()) {
  const store = new FakeStageStore();
  store.bind("agent", record);
  const runner = new FakePiRunner(store);
  const provider = new PiCloudAgentProvider(store, runner, () => 1);
  const launch = (id = "agent") =>
    provider.launch({ idempotencyKey: id, prompt: record.input }, context);
  return { store, runner, provider, launch };
}
