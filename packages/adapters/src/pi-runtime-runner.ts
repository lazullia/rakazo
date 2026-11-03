import { createHash, randomUUID } from "node:crypto";
import type { AgentRunModel, AgentRuntime, ModelCallObserver } from "@rakazo/adapter-kit";
import type {
  PiObservation,
  PiStageRecord,
  PiStageRunner,
  PiStageStore,
} from "./pi-cloud-agent.js";
import { piNativeStageId, piStageIdentity } from "./pi-native-admission.js";
import { PiRuntimeExecution } from "./pi-runtime-execution.js";

/** Existing stage JSON/row lock is the runtime's durable start slot. An accepted
 * slot is never stolen/replayed after restart, even if its deadline has passed. */
export class PiRuntimeRunner implements PiStageRunner {
  private readonly owner = randomUUID();
  private readonly preparing = new Map<
    string,
    { identity: string; result: Promise<PiObservation> }
  >();
  private readonly active = new Map<string, PiRuntimeExecution>();
  constructor(
    private readonly store: PiStageStore,
    readonly namespace: string,
    private readonly deps: {
      model: { provider: string; id: string; connectionId?: string };
      authorized(id: string): Promise<boolean>;
      resolveModel(record: Readonly<PiStageRecord>): Promise<AgentRunModel>;
      createRuntime?(observer: ModelCallObserver): Promise<AgentRuntime>;
      now?: () => number;
    },
  ) {}
  private now() {
    return this.deps.now?.() ?? Date.now();
  }
  private valid(record: PiStageRecord, handle: string) {
    return (
      record.runtimeProtocol === "pi-runtime/v1" &&
      record.handle === handle &&
      piNativeStageId(record) === handle &&
      record.runnerNamespace === this.namespace
    );
  }
  async startReserved(
    record: Readonly<PiStageRecord>,
    limits: Parameters<PiStageRunner["startReserved"]>[1],
  ): Promise<PiObservation> {
    if (
      !this.valid(record, record.handle) ||
      limits.maxInferences !== 10 ||
      limits.maxOutputTokens !== 1024 ||
      limits.maxDurationMs !== 120000
    )
      return { kind: "unknown" };
    const pending = this.preparing.get(record.handle);
    if (pending)
      return pending.identity === piStageIdentity(record) ? pending.result : { kind: "unknown" };
    const result = this.start(record).catch(() => ({ kind: "unknown" as const }));
    this.preparing.set(record.handle, { identity: piStageIdentity(record), result });
    try {
      return await result;
    } finally {
      this.preparing.delete(record.handle);
    }
  }
  private async start(record: Readonly<PiStageRecord>): Promise<PiObservation> {
    if (!(await this.deps.authorized(record.handle))) return this.cancel(record.handle);
    const accepted = await this.store.change(record.handle, (saved) => {
      if (!this.valid(saved, record.handle) || piStageIdentity(saved) !== piStageIdentity(record))
        throw Error("Invalid Pi stage.");
      if (
        saved.runtimeClaim ||
        saved.cancelRequested ||
        !saved.approvalValid ||
        !saved.sharingAllowed ||
        saved.state === "unknown" ||
        saved.stopConfirmed ||
        this.now() >= saved.startUntil ||
        this.now() >= saved.workUntil
      )
        return false;
      if (
        !saved.dispatched ||
        !saved.dispatchClaim ||
        saved.dispatchClaim.owner !== record.dispatchClaim?.owner ||
        saved.dispatchClaim.generation !== record.dispatchClaim?.generation
      )
        throw Error("Invalid Pi claim.");
      saved.runtimeClaim = {
        owner: this.owner,
        generation: saved.dispatchGeneration,
        until: Math.min(this.now() + 120000, saved.workUntil),
      };
      return true;
    });
    if (!accepted) return this.lookup(record.handle);
    const current = await this.store.read(record.handle);
    if (!current || current.runtimeClaim?.owner !== this.owner) return { kind: "unknown" };
    if (current.cancelRequested || !(await this.deps.authorized(record.handle))) {
      await this.store.change(record.handle, (r) => {
        if (r.runtimeClaim?.owner !== this.owner) throw Error("Invalid Pi claim.");
        r.cancelRequested = true;
        r.state = "cancelled";
        r.stopConfirmed = true;
      });
      return { kind: "cancelled", stopConfirmed: true };
    }
    // A concurrent cancel may commit here. Before every upstream call, the body
    // checks the current store/approval again; cancel stays pending until settled.
    const execution = new PiRuntimeExecution(current, {
      namespace: this.namespace,
      model: this.deps.model,
      now: this.deps.now,
      authorized: async () => {
        const r = await this.store.read(current.handle);
        return Boolean(
          r &&
            r.runtimeClaim?.owner === this.owner &&
            !r.cancelRequested &&
            r.state !== "unknown" &&
            (await this.deps.authorized(current.handle)),
        );
      },
      resolveModel: () => this.deps.resolveModel(current),
      createRuntime: this.deps.createRuntime,
    });
    this.active.set(current.handle, execution);
    void execution
      .execute()
      .then(async (observation) => {
        try {
          await this.save(current.handle, observation);
        } finally {
          // UNKNOWN is not stop evidence. Keep the owning execution reachable
          // until its work has actually settled, including timeout/abort races.
          await execution.waitForStop();
          const latest = await this.store.read(current.handle);
          if (latest?.cancelRequested) await this.save(current.handle, await execution.abort());
          else if (observation.kind === "unknown")
            await this.store.change(current.handle, (r) => {
              if (r.runtimeClaim?.owner !== this.owner) throw Error("Invalid Pi owner.");
              r.stopConfirmed = execution.diagnostics().stopped;
            });
        }
      })
      .catch(() => undefined)
      .finally(() => {
        if (this.active.get(current.handle) === execution) this.active.delete(current.handle);
      });
    return { kind: "running" };
  }
  private async save(handle: string, observation: PiObservation) {
    await this.store.change(handle, (r) => {
      if (r.runtimeClaim?.owner !== this.owner) throw Error("Invalid Pi owner.");
      if (r.state === "cancelled" && r.stopConfirmed) return;
      if (observation.kind === "completed") {
        if (observation.targetHash !== r.targetHash) {
          r.state = "unknown";
          return;
        }
        const hash = createHash("sha256").update(observation.text).digest("hex");
        if (r.original && r.original.hash !== hash) {
          r.state = "unknown";
          return;
        }
        r.original ??= { text: observation.text, hash, targetHash: observation.targetHash };
        r.stopConfirmed = true;
        if (r.cancelRequested) r.state = "cancelled";
        else if (r.state !== "unknown") r.state = "completed";
      } else if (observation.kind === "cancelled" && observation.stopConfirmed) {
        r.state = "cancelled";
        r.stopConfirmed = true;
      } else {
        r.state = "unknown";
        r.reason = r.cancelRequested ? "cancel-pending" : "execution-unknown";
      }
    });
  }
  async lookup(handle: string): Promise<PiObservation> {
    try {
      const r = await this.store.read(handle);
      if (!r || !this.valid(r, handle)) return { kind: "unknown" };
      if (r.state === "cancelled" && r.stopConfirmed)
        return { kind: "cancelled", stopConfirmed: true };
      if (r.state === "unknown") return { kind: "unknown" };
      if (
        r.state === "completed" &&
        r.stopConfirmed &&
        r.original &&
        !r.cancelRequested &&
        r.original.targetHash === r.targetHash &&
        createHash("sha256").update(r.original.text).digest("hex") === r.original.hash
      )
        return {
          kind: "completed",
          text: r.original.text,
          ...(r.targetHash ? { targetHash: r.targetHash } : {}),
        };
      if (r.cancelRequested) return { kind: "cancelled", stopConfirmed: false };
      return r.runtimeClaim && this.now() < r.runtimeClaim.until
        ? { kind: "running" }
        : { kind: "unknown" };
    } catch {
      return { kind: "unknown" };
    }
  }
  async cancel(handle: string): Promise<PiObservation> {
    try {
      await this.store.change(handle, (r) => {
        if (!this.valid(r, handle)) throw Error("Invalid Pi stage.");
        r.cancelRequested = true;
        if (!r.runtimeClaim || r.stopConfirmed) {
          r.state = "cancelled";
          r.stopConfirmed = true;
        }
      });
      const execution = this.active.get(handle);
      if (execution) await this.save(handle, await execution.abort());
      return this.lookup(handle);
    } catch {
      return { kind: "unknown" };
    }
  }
}
