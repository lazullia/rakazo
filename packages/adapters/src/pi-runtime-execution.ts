import type { AgentRunModel, AgentRuntime, ModelCallObserver } from "@rakazo/adapter-kit";
import type { PiObservation, PiStageRecord } from "./pi-cloud-agent.js";

/** One already-admitted execution body. This is deliberately NOT PiStageRunner:
 * the caller must durably own the stage's start slot before constructing it.
 * Local lookup/abort cannot recover another process or prove a missing job stopped. */
export class PiRuntimeExecution {
  private readonly controller = new AbortController();
  private runtime?: AgentRuntime;
  private work?: Promise<void>;
  private completion?: Promise<PiObservation>;
  private resolve?: (value: PiObservation) => void;
  private observation: PiObservation = { kind: "running" };
  private cancelled = false;
  private stopped = false;
  private calls = 0;
  private reason?: "authorization" | "model" | "limit" | "timeout" | "runtime";
  private timer?: ReturnType<typeof setTimeout>;

  constructor(
    private readonly record: Readonly<PiStageRecord>,
    private readonly deps: {
      namespace: string;
      model: { provider: string; id: string; connectionId?: string };
      /** Current server receipt, never MCP role self-assertion. Read-only. */
      authorized(): Promise<boolean>;
      resolveModel(): Promise<AgentRunModel>;
      createRuntime?(observer: ModelCallObserver): Promise<AgentRuntime>;
      now?: () => number;
    },
  ) {
    // The execution owns its immutable snapshot, not the caller's mutable object.
    this.record = structuredClone(record);
  }

  diagnostics() {
    return { inferences: this.calls, stopped: this.stopped, reason: this.reason };
  }

  lookup(): PiObservation {
    return structuredClone(this.observation);
  }

  execute(): Promise<PiObservation> {
    if (this.completion) return this.completion;
    if (this.cancelled) {
      this.completion = Promise.resolve(this.lookup());
      return this.completion;
    }
    this.completion = new Promise((resolve) => {
      this.resolve = resolve;
    });
    const remaining = Math.min(120_000, this.record.workUntil - this.now());
    if (!Number.isFinite(remaining) || remaining <= 0) {
      this.reason = "authorization";
      this.stopped = true;
      this.finish({ kind: "unknown" });
      return this.completion;
    }
    this.timer = setTimeout(() => {
      this.reason = "timeout";
      this.controller.abort();
      void this.runtime?.abort(this.record.handle).catch(() => undefined);
      // A deadline is not proof of termination. Late text never repairs UNKNOWN.
      this.finish({ kind: "unknown" });
    }, remaining);
    this.timer.unref?.();
    this.work = this.run();
    return this.completion;
  }

  /** Settles only when this execution's runtime iteration has actually exited. */
  waitForStop(): Promise<void> {
    return this.work ?? Promise.resolve();
  }

  async abort(): Promise<PiObservation> {
    this.cancelled = true;
    this.controller.abort();
    if (!this.work) {
      this.stopped = true;
      this.finish({ kind: "cancelled", stopConfirmed: true });
      return this.lookup();
    }
    const stopping = Promise.all([
      this.work,
      this.runtime?.abort(this.record.handle) ?? Promise.resolve(),
    ]).then(
      () => true,
      () => false,
    );
    let timer: ReturnType<typeof setTimeout> | undefined;
    const settled = await Promise.race([
      stopping,
      new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), 60_000);
        timer.unref?.();
      }),
    ]);
    if (timer) clearTimeout(timer);
    if (settled && this.stopped) this.finish({ kind: "cancelled", stopConfirmed: true }, true);
    else this.finish({ kind: "unknown" });
    return this.lookup();
  }

  private now() {
    return this.deps.now?.() ?? Date.now();
  }

  private finish(value: PiObservation, confirmedCancellation = false) {
    if (this.observation.kind !== "running" && !confirmedCancellation) return;
    this.observation = value;
    this.resolve?.(structuredClone(value));
  }

  private async check(starting = false) {
    this.controller.signal.throwIfAborted();
    const r = this.record;
    if (
      !Number.isFinite(r.startUntil) ||
      !Number.isFinite(r.workUntil) ||
      !r.handle ||
      (r.stage === "review"
        ? !/^[a-f0-9]{64}$/.test(r.targetHash ?? "")
        : r.targetHash !== undefined) ||
      !r.dispatched ||
      r.dispatchGeneration < 1 ||
      !["reserved", "running"].includes(r.state) ||
      r.stopConfirmed ||
      r.original ||
      r.reconciliationExpired ||
      (starting && this.now() >= r.startUntil) ||
      !r.approvalValid ||
      !r.sharingAllowed ||
      r.cancelRequested ||
      r.runnerNamespace !== this.deps.namespace ||
      this.now() >= r.workUntil ||
      !(await this.deps.authorized())
    ) {
      this.reason = "authorization";
      throw new Error("Pi execution refused.");
    }
    if (this.now() >= r.workUntil || (starting && this.now() >= r.startUntil)) {
      this.reason = "authorization";
      throw new Error("Pi execution refused.");
    }
    this.controller.signal.throwIfAborted();
  }

  private async run() {
    try {
      await this.check(true);
      const model = await this.deps.resolveModel();
      if (
        model.provider !== this.deps.model.provider ||
        model.id !== this.deps.model.id ||
        model.connectionId !== this.deps.model.connectionId
      ) {
        this.reason = "model";
        throw new Error("Pi execution refused.");
      }
      const outputLimit = Math.min(1024, model.maxTokens ?? 1024);
      if (!Number.isInteger(outputLimit) || outputLimit <= 0) {
        this.reason = "limit";
        throw new Error("Pi execution refused.");
      }
      const accounting = model.callObserver;
      const observer: ModelCallObserver = {
        beforeCall: async (call) => {
          await this.check();
          if (
            call.runId !== this.record.handle ||
            call.provider !== model.provider ||
            call.modelId !== model.id ||
            !Number.isInteger(call.maxOutputTokens) ||
            call.maxOutputTokens > 1024 ||
            call.maxOutputTokens <= 0 ||
            this.calls >= 10
          ) {
            this.reason = "limit";
            throw new Error("Pi execution refused.");
          }
          this.calls += 1;
          return accounting ? accounting.beforeCall(call) : `pi-call:${this.calls}`;
        },
        afterCall: async (id, usage) => {
          await accounting?.afterCall(id, usage);
        },
        onRequestId: async (id, requestId) => {
          await accounting?.onRequestId?.(id, requestId);
        },
      };
      this.runtime = this.deps.createRuntime
        ? await this.deps.createRuntime(observer)
        : new (await import("./pi-runtime.js")).PiAgentRuntime({ modelCallObserver: observer });
      await this.check(true);
      let text = "",
        done = false;
      for await (const event of this.runtime.run(
        {
          runId: this.record.handle,
          botId: this.record.botId,
          threadId: this.record.threadId,
          prompt: this.record.input,
          instructions:
            "Complete only the delegated design or review task. Return the result and unresolved concerns. Completion is not acceptance.",
          history: [],
          tools: [],
          // An empty model response is not the runtime's synthetic fallback result.
          allowSilentEmpty: true,
          model: { ...model, maxTokens: outputLimit, callObserver: observer },
        },
        {
          userId: this.record.ownerId,
          spaceId: this.record.spaceId,
          botId: this.record.botId,
          operationId: this.record.handle,
          traceId: this.record.handle,
          signal: this.controller.signal,
        },
      )) {
        this.controller.signal.throwIfAborted();
        if (event.type === "text") {
          text += event.text;
          if (Buffer.byteLength(text) > 65_536) {
            this.reason = "limit";
            throw new Error("Pi execution refused.");
          }
        }
        if (event.type === "done") done = true;
      }
      if (!done || !text.trim() || !this.calls) throw new Error("Pi execution incomplete.");
      this.finish({
        kind: "completed",
        text,
        ...(this.record.targetHash ? { targetHash: this.record.targetHash } : {}),
      });
    } catch {
      this.reason ??= "runtime";
      this.finish({ kind: "unknown" });
    } finally {
      if (this.timer) clearTimeout(this.timer);
      this.stopped = true;
      // execute's UNKNOWN remains sticky; abort can separately confirm termination.
    }
  }
}
