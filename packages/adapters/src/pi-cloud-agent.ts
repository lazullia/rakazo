import { createHash, randomUUID } from "node:crypto";
import type {
  AdapterContext,
  CloudAgentHandle,
  CloudAgentLaunchRequest,
  CloudAgentProvider,
  CloudAgentReplyRequest,
  CloudAgentSnapshot,
} from "@rakazo/adapter-kit";
import { CloudAgentRequestRejected } from "@rakazo/adapter-kit";
import { piNativeStageId, piResultReferenceText } from "./pi-native-admission.js";

export type PiStage = "design" | "review";
export type PiObservation =
  | { kind: "running" }
  | { kind: "missing" | "unknown" }
  | { kind: "cancelled"; stopConfirmed: boolean }
  | { kind: "completed"; text: string; targetHash?: string };

/** Trusted server record, never supplied by tool arguments or an MCP bearer. */
export interface PiStageRecord {
  handle: string;
  /** Stable identity of the configured execution backend, never a tool argument. */
  runnerNamespace: string;
  workId: string;
  stage: PiStage;
  ownerId: string;
  spaceId: string;
  botId: string;
  threadId: string;
  originRunId: string;
  approvalRef: string;
  approvalValid: boolean;
  sharingAllowed: boolean;
  /** Admission only; does not cancel an already dispatched execution. */
  startUntil: number;
  workUntil: number;
  stopReconcileUntil: number;
  dispatchGeneration: number;
  dispatchClaim?: { owner: string; generation: number; until: number };
  reconciliationExpired?: boolean;
  /** Durable runtime acceptance; never expires into permission to start again. */
  runtimeProtocol?: "pi-runtime/v1";
  runtimeClaim?: { owner: string; generation: number; until: number };
  input: string;
  targetHash?: string;
  returnTo: { botId: string; threadId: string };
  supervisor?: { botId: string; threadId: string; requestMessageId: string };
  dispatched: boolean;
  cancelRequested: boolean;
  state: "reserved" | "running" | "completed" | "unknown" | "cancelled";
  reason?: "execution-unknown" | "target-mismatch" | "cancel-pending";
  stopConfirmed: boolean;
  original?: { text: string; hash: string; targetHash?: string };
}

/**
 * Production adapter must durably resolve every CloudAgent alias to one work/stage,
 * and serialize mutations across processes, including the persisted dispatch claim.
 * The callback must not perform I/O.
 * Registration/authentication, alias admission and retention are server responsibilities.
 */
export interface PiStageStore {
  read(agentId: string): Promise<PiStageRecord | undefined>;
  change<T>(
    agentId: string,
    update: (record: PiStageRecord, design?: PiStageRecord) => T,
  ): Promise<T>;
}

/**
 * Runner boundary: startReserved consumes one durable reservation at most once and
 * rechecks revocation/deadline at its start/cancel serialization point. Cancel must
 * tombstone an unstarted reservation; only confirmed termination returns cancelled.
 * completed requires verified execution termination and a saved original.
 * lookup MUST NOT create. No HTTP or live runner implementation is supplied here.
 */
export interface PiStageRunner {
  readonly namespace: string;
  startReserved(
    record: Readonly<PiStageRecord>,
    limits: {
      maxInferences: 10;
      maxOutputTokens: 1024;
      maxDurationMs: 120_000;
    },
  ): Promise<PiObservation>;
  lookup(handle: string): Promise<PiObservation>;
  cancel(handle: string): Promise<PiObservation>;
}

/** threadId comes from the authenticated server Run, never from tool arguments. */
export type PiHandoffContext = AdapterContext & { threadId: string };
export interface PiHandoffTarget {
  workId: string;
  stage: PiStage;
  originalHash: string;
  targetHash?: string;
}

/** Explicitly injected only; deliberately absent from live environment composition. */
export class PiCloudAgentProvider implements CloudAgentProvider {
  private readonly dispatchOwner = randomUUID();
  constructor(
    private readonly store: PiStageStore,
    private readonly runner: PiStageRunner,
    private readonly now: () => number = Date.now,
  ) {}

  describe() {
    return {
      id: "pi",
      contractVersion: "1",
      adapterVersion: "0.1.0",
      capabilities: { launch: true, reply: false, cancel: true },
    };
  }

  private checkOwner(record: PiStageRecord, context: AdapterContext) {
    if (
      record.runnerNamespace !== this.runner.namespace ||
      record.ownerId !== context.userId ||
      record.spaceId !== context.spaceId ||
      !record.approvalRef ||
      !record.originRunId
    )
      throw new CloudAgentRequestRejected("invalid");
  }

  private check(record: PiStageRecord, context: AdapterContext) {
    this.checkOwner(record, context);
    if (record.botId !== context.botId) throw new CloudAgentRequestRejected("invalid");
  }

  private liveDispatch(record: PiStageRecord) {
    return Boolean(record.dispatchClaim && this.now() < record.dispatchClaim.until);
  }

  private designEligible(record: PiStageRecord, design?: PiStageRecord) {
    return (
      record.stage !== "review" ||
      Boolean(
        design &&
          design.ownerId === record.ownerId &&
          design.spaceId === record.spaceId &&
          design.state === "completed" &&
          design.original?.hash === record.targetHash &&
          record.targetHash &&
          design.approvalValid &&
          design.sharingAllowed &&
          !design.cancelRequested &&
          this.now() < design.workUntil,
      )
    );
  }

  private async record(id: string, context: AdapterContext) {
    const record = await this.store.read(id);
    if (!record) throw new CloudAgentRequestRejected("invalid");
    this.check(record, context);
    return record;
  }

  /** Server-only scheduling hint. Current approval is still checked separately. */
  async remainingWorkTime(id: string, context: AdapterContext) {
    const record = await this.record(id, context);
    return Math.max(0, record.workUntil - this.now());
  }

  async checkNativeThread(id: string, context: AdapterContext, threadId: string) {
    const record = await this.record(id, context);
    if (record.threadId !== threadId) throw new CloudAgentRequestRejected("invalid");
  }

  async launch(
    request: CloudAgentLaunchRequest,
    context: AdapterContext,
  ): Promise<CloudAgentHandle> {
    context.signal?.throwIfAborted();
    const id = request.idempotencyKey;
    const bound = await this.record(id, context);
    if (
      request.prompt !== bound.input ||
      request.repository ||
      request.images?.length ||
      request.openPr
    ) {
      throw new CloudAgentRequestRejected("invalid");
    }
    return this.reconcile(id, context, false);
  }

  /** Native poll uses this before its generic retry/cancellation branches. */
  async reconcile(
    id: string,
    context: AdapterContext,
    cancelRequested: boolean,
  ): Promise<CloudAgentSnapshot> {
    context.signal?.throwIfAborted();
    await this.record(id, context);
    const action = await this.store.change(id, (record, design) => {
      this.check(record, context);
      // Saved execution evidence remains completed; return authorization is separate.
      if (
        record.original &&
        record.state === "completed" &&
        !record.cancelRequested &&
        !cancelRequested
      ) {
        return { kind: "saved" as const, record: structuredClone(record) };
      }
      if (
        cancelRequested ||
        !record.approvalValid ||
        !record.sharingAllowed ||
        !this.designEligible(record, design) ||
        this.now() >= record.workUntil ||
        (!record.dispatched && this.now() >= record.startUntil)
      ) {
        record.cancelRequested = true;
      }
      if (record.state === "unknown" && this.now() >= record.stopReconcileUntil) {
        record.reconciliationExpired = true;
        return { kind: "saved" as const, record: structuredClone(record) };
      }
      if (record.cancelRequested)
        return { kind: "cancel" as const, record: structuredClone(record) };
      // Another owner is still awaiting the one start response. Missing visibility
      // is not execution uncertainty until this persisted, bounded claim expires.
      if (this.liveDispatch(record))
        return { kind: "saved" as const, record: structuredClone(record) };
      if (record.state === "unknown" || record.dispatched)
        return { kind: "lookup" as const, record: structuredClone(record) };
      // Intent before I/O: an ambiguous dispatch can only be looked up, never relaunched.
      record.dispatched = true;
      record.dispatchGeneration += 1;
      record.dispatchClaim = {
        owner: this.dispatchOwner,
        generation: record.dispatchGeneration,
        until: Math.min(this.now() + 60_000, record.workUntil),
      };
      return { kind: "start" as const, record: structuredClone(record) };
    });
    if (action.kind === "saved") return this.snapshot(id, action.record);
    let observation: PiObservation;
    try {
      observation =
        action.kind === "start"
          ? await this.runner.startReserved(action.record, {
              maxInferences: 10,
              maxOutputTokens: 1024,
              maxDurationMs: 120_000,
            })
          : action.kind === "cancel"
            ? await this.runner.cancel(action.record.handle)
            : await this.runner.lookup(action.record.handle);
    } catch {
      // No provider exception body, prompt or credentials enter the saved error.
      observation = { kind: "unknown" };
    }
    return this.observe(
      id,
      context,
      observation,
      action.kind === "start" ? action.record.dispatchClaim : undefined,
    );
  }

  private async observe(
    id: string,
    context: AdapterContext,
    observation: PiObservation,
    claim?: PiStageRecord["dispatchClaim"],
  ) {
    return this.store.change(id, (record) => {
      this.check(record, context);
      // Do not regress a confirmed cancellation on an old in-flight observation.
      if (record.state === "cancelled" && record.stopConfirmed) return this.snapshot(id, record);
      if (
        record.state === "completed" &&
        !record.cancelRequested &&
        observation.kind !== "completed"
      ) {
        return this.snapshot(id, record);
      }
      if (claim) {
        // Only the matching dispatch generation may settle its own response.
        if (
          record.dispatchClaim?.owner !== claim.owner ||
          record.dispatchClaim.generation !== claim.generation
        ) {
          return this.snapshot(id, record);
        }
        if (this.now() >= claim.until) {
          record.state = "unknown";
          record.reason = "execution-unknown";
        }
        record.dispatchClaim = undefined;
      } else if (
        !record.cancelRequested &&
        this.liveDispatch(record) &&
        (observation.kind === "missing" || observation.kind === "unknown")
      ) {
        return this.snapshot(id, record);
      }
      const held = record.state === "unknown";
      if (observation.kind === "cancelled" && observation.stopConfirmed) {
        record.state = "cancelled";
        record.dispatchClaim = undefined;
        record.stopConfirmed = true;
        record.reason = undefined;
      } else if (observation.kind === "completed") {
        const hash = createHash("sha256").update(observation.text).digest("hex");
        if (
          record.targetHash !== observation.targetHash ||
          (record.original && record.original.hash !== hash)
        ) {
          record.state = "unknown";
          record.reason = "target-mismatch";
        } else {
          record.dispatchClaim = undefined;
          record.original ??= { text: observation.text, hash, targetHash: observation.targetHash };
          record.stopConfirmed = true;
          if (record.cancelRequested) {
            record.state = "cancelled";
            record.reason = undefined;
          } else if (!held) {
            record.state = "completed";
            record.reason = undefined;
          }
          // A late result may be retained, but never automatically releases an UNKNOWN hold.
        }
      } else if (observation.kind === "running" && !held && !record.cancelRequested) {
        record.state = "running";
      } else {
        record.state = "unknown";
        record.reason = record.cancelRequested ? "cancel-pending" : "execution-unknown";
      }
      return this.snapshot(id, record);
    });
  }

  private snapshot(id: string, record: PiStageRecord): CloudAgentSnapshot {
    return {
      id,
      url: "",
      title: "Pi task",
      status:
        record.state === "completed"
          ? "finished"
          : record.state === "cancelled"
            ? "cancelled"
            : record.state === "unknown"
              ? "failed"
              : "running",
    };
  }

  async get(id: string, context: AdapterContext): Promise<CloudAgentSnapshot> {
    context.signal.throwIfAborted();
    const record = await this.record(id, context);
    if (record.original && record.state === "completed" && !record.cancelRequested)
      return this.snapshot(id, record);
    if (record.state === "unknown" && this.now() >= record.stopReconcileUntil) {
      return this.store.change(id, (current) => {
        this.check(current, context);
        current.reconciliationExpired = true;
        return this.snapshot(id, current);
      });
    }
    if (!record.cancelRequested && this.liveDispatch(record)) return this.snapshot(id, record);
    // get is observation-only, including an unstarted/missing operation.
    let observation: PiObservation;
    try {
      observation = await this.runner.lookup(record.handle);
    } catch {
      observation = { kind: "unknown" };
    }
    return this.observe(id, context, observation);
  }

  async cancel(id: string, context: AdapterContext): Promise<CloudAgentSnapshot> {
    return this.reconcile(id, context, true);
  }

  async reply(
    _id: string,
    _request: CloudAgentReplyRequest,
    _context: AdapterContext,
  ): Promise<CloudAgentHandle> {
    throw new CloudAgentRequestRejected("invalid");
  }

  private result(record: PiStageRecord) {
    return structuredClone({
      workId: record.workId,
      stage: record.stage,
      originRunId: record.originRunId,
      executionState: record.state,
      reason: record.reason,
      stopConfirmed: record.stopConfirmed,
      reconciliationExpired: record.reconciliationExpired ?? false,
      original: record.original,
      supervisor: record.supervisor,
      ...(record.stage === "review" && record.supervisor && record.original
        ? {
            supervisorReturn: {
              botId: record.supervisor.botId,
              threadId: record.supervisor.threadId,
              message: piResultReferenceText(
                piNativeStageId(record),
                "review",
                record.original.hash,
              ),
            },
          }
        : {}),
      ...(record.stage === "review"
        ? { designId: piNativeStageId({ ...record, stage: "design" }) }
        : {}),
      accepted: false as const,
    });
  }

  /** Executor-only diagnostics, also usable to report a held/expired execution. */
  async readResult(id: string, context: AdapterContext) {
    return this.result(await this.record(id, context));
  }

  /** Narrow read-only handoff. Caller identity/thread are server-derived, not impersonated. */
  async readHandoff(id: string, context: PiHandoffContext, target: PiHandoffTarget) {
    context.signal.throwIfAborted();
    return this.store.change(id, (record, design) => {
      this.checkOwner(record, context);
      if (
        record.returnTo.botId !== context.botId ||
        record.returnTo.threadId !== context.threadId ||
        !record.approvalValid ||
        !record.sharingAllowed ||
        record.cancelRequested ||
        this.now() >= record.workUntil ||
        record.state !== "completed" ||
        !record.original ||
        !this.designEligible(record, design) ||
        target.workId !== record.workId ||
        target.stage !== record.stage ||
        target.originalHash !== record.original.hash ||
        target.targetHash !== record.original.targetHash
      )
        throw new CloudAgentRequestRejected("invalid");
      return this.result(record);
    });
  }

  /** Model supplies only an id. Work/recipient/target are resolved from server state. */
  async readForRun(id: string, context: PiHandoffContext) {
    context.signal.throwIfAborted();
    return this.store.change(id, (record, design) => {
      this.checkOwner(record, context);
      const executor = record.botId === context.botId && record.threadId === context.threadId;
      const recipient =
        record.returnTo.botId === context.botId && record.returnTo.threadId === context.threadId;
      const supervisor =
        record.supervisor?.botId === context.botId &&
        record.supervisor?.threadId === context.threadId;
      const original = record.original;
      if (
        (!executor && !recipient && !supervisor) ||
        !record.approvalValid ||
        !record.sharingAllowed ||
        record.cancelRequested ||
        this.now() >= record.workUntil ||
        record.state !== "completed" ||
        !original ||
        !this.designEligible(record, design) ||
        original.targetHash !== record.targetHash ||
        createHash("sha256").update(original.text).digest("hex") !== original.hash
      ) {
        throw new CloudAgentRequestRejected("invalid");
      }
      const result = this.result(record);
      return {
        ...result,
        ...(executor && original
          ? {
              approvedReturn: {
                botId: record.returnTo.botId,
                threadId: record.returnTo.threadId,
                message: piResultReferenceText(id, record.stage, original.hash),
              },
            }
          : {}),
      };
    });
  }

  /** Server-only delivery metadata; never expose cached original text to a sender. */
  async returnReference(
    id: string,
    context: AdapterContext & { botId: string },
    current: { botId: string; threadId: string },
  ) {
    const result = await this.readForRun(id, {
      ...context,
      threadId: (await this.record(id, context)).threadId,
    });
    const returnTo = await this.returnRoute(id, context, current);
    if (!result.original) throw new CloudAgentRequestRejected("invalid");
    return {
      id,
      workId: result.workId,
      stage: result.stage,
      originalHash: result.original.hash,
      targetHash: result.original.targetHash,
      returnTo,
    };
  }

  /** Narrow final design-reader -> approved original supervisor reference. */
  async supervisorReference(
    id: string,
    context: PiHandoffContext,
    target: { botId: string; threadId: string },
  ) {
    return this.store.change(id, (record, design) => {
      this.checkOwner(record, context);
      if (
        record.stage !== "review" ||
        record.returnTo.botId !== context.botId ||
        record.returnTo.threadId !== context.threadId ||
        record.supervisor?.botId !== target.botId ||
        record.supervisor.threadId !== target.threadId ||
        record.state !== "completed" ||
        !record.stopConfirmed ||
        !record.original ||
        record.original.targetHash !== record.targetHash ||
        !record.approvalValid ||
        !record.sharingAllowed ||
        record.cancelRequested ||
        record.reconciliationExpired ||
        this.now() >= record.workUntil ||
        !this.designEligible(record, design) ||
        createHash("sha256").update(record.original.text).digest("hex") !== record.original.hash
      )
        throw new CloudAgentRequestRejected("invalid");
      return {
        id,
        workId: record.workId,
        stage: record.stage,
        originalHash: record.original.hash,
        targetHash: record.targetHash,
        returnTo: structuredClone(target),
      };
    });
  }

  async returnRoute(
    id: string,
    context: AdapterContext,
    current: { botId: string; threadId: string },
  ) {
    await this.record(id, context);
    return this.store.change(id, (record, design) => {
      this.check(record, context);
      if (
        !record.approvalValid ||
        !record.sharingAllowed ||
        record.cancelRequested ||
        this.now() >= record.workUntil ||
        record.state !== "completed" ||
        !record.original ||
        !this.designEligible(record, design) ||
        current.botId !== record.returnTo.botId ||
        current.threadId !== record.returnTo.threadId
      )
        throw new CloudAgentRequestRejected("invalid");
      return structuredClone(record.returnTo);
    });
  }
}
