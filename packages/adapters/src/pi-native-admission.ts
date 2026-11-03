import { createHash } from "node:crypto";
import type { AdapterContext, CloudAgentLaunchRequest } from "@rakazo/adapter-kit";
import type { PrismaClient } from "@rakazo/db";
import type { PiStage, PiStageRecord, PiStageStore } from "./pi-cloud-agent.js";

/** Server-side receipt storage, never an MCP/tool argument or model assertion.
 * Implementations must bind Runs from authenticated launch/continuation/delivery
 * records, preserve one immutable decision per work, retain revocation tombstones,
 * and serialize registration. A failed lookup must throw, not masquerade as an unbound Run.
 * No production receipt/authentication adapter is supplied by this candidate.
 */
export interface PiWorkApprovalSource {
  forRun(runId: string): Promise<PiApprovedStage | undefined>;
  forStage(agentId: string): Promise<PiApprovedStage | undefined>;
}
export interface PiApprovedStage {
  decisionId: string;
  approvedBy: string;
  scope: "generalized-task-and-results";
  record: PiStageRecord;
  /** Server-derived Run access; receipt of an original is not execution consent. */
  runAccess?: "execute" | "read";
}
export interface PiStageRegistrar extends PiStageStore {
  bindTrustedStage(agentId: string, record: PiStageRecord): Promise<void>;
}
export const PI_ADMISSION_ERROR = "Pi work is not authorized for this run.";
function refuse(): never {
  throw new Error(PI_ADMISSION_ERROR);
}

/** Different operation keys/Runs share the same native and runner start slot. */
export function piNativeStageId(
  record: Pick<PiStageRecord, "ownerId" | "spaceId" | "workId" | "stage">,
) {
  return `pi-${createHash("sha256")
    .update(JSON.stringify([record.ownerId, record.spaceId, record.workId, record.stage]))
    .digest("hex")}`;
}
export function piStageIdentity(record: PiStageRecord) {
  return JSON.stringify([
    record.ownerId,
    record.spaceId,
    record.workId,
    record.stage,
    record.runnerNamespace,
    record.runtimeProtocol,
    record.handle,
    record.botId,
    record.threadId,
    record.originRunId,
    record.approvalRef,
    record.input,
    record.targetHash,
    record.returnTo,
    record.supervisor,
    record.startUntil,
    record.workUntil,
    record.stopReconcileUntil,
  ]);
}

/** Canonical metadata-only payload; callers submit these exact bytes through the gate. */
export function piResultReferenceText(id: string, stage: PiStage, hash: string) {
  return `Pi ${stage} original: ${id}; sha256 ${hash}. Use cloud_agent_result to read it. Acceptance has not been determined.`;
}

/** Internal admission after the ordinary cloud_agent_launch/message_bot gates.
 * The source is an authentication boundary, not a way to turn model text into consent.
 */
export class PiNativeAdmission {
  constructor(
    private readonly source: PiWorkApprovalSource,
    private readonly store: PiStageRegistrar,
    private readonly now: () => number = Date.now,
  ) {}

  /** Synchronous final guard: call after the last authorization await. */
  withinWorkDeadline(record: Pick<PiStageRecord, "workUntil">) {
    return Number.isFinite(record.workUntil) && this.now() < record.workUntil;
  }

  async forRun(context: AdapterContext & { botId: string }, run: { id: string; threadId: string }) {
    const receipt = await this.source.forRun(run.id).catch(refuse);
    if (!receipt) return undefined;
    const record = structuredClone(receipt.record);
    if (
      (receipt.runAccess !== undefined && receipt.runAccess !== "execute") ||
      !receipt.decisionId ||
      receipt.decisionId !== record.approvalRef ||
      receipt.approvedBy !== context.userId ||
      receipt.scope !== "generalized-task-and-results" ||
      record.ownerId !== context.userId ||
      record.spaceId !== context.spaceId ||
      record.botId !== context.botId ||
      record.threadId !== run.threadId ||
      !record.approvalValid ||
      !record.sharingAllowed ||
      record.cancelRequested ||
      this.now() >= record.workUntil ||
      !record.workId ||
      !record.originRunId ||
      !Number.isFinite(record.startUntil) ||
      !Number.isFinite(record.workUntil) ||
      !Number.isFinite(record.stopReconcileUntil) ||
      record.workUntil > record.stopReconcileUntil
    )
      refuse();
    return { id: piNativeStageId(record), record };
  }

  /** Queued work must re-read authoritative consent before the poll may start it.
   * Missing/failed lookup blocks dispatch; the poll requests cancellation and
   * relies on runner stop evidence, rather than claiming missing means stopped.
   */
  async currentStage(id: string) {
    try {
      const receipt = await this.source.forStage(id);
      const saved = await this.store.read(id);
      return Boolean(
        receipt &&
          saved &&
          piNativeStageId(receipt.record) === id &&
          receipt.decisionId === saved.approvalRef &&
          receipt.approvedBy === saved.ownerId &&
          receipt.scope === "generalized-task-and-results" &&
          piStageIdentity(receipt.record) === piStageIdentity(saved) &&
          receipt.record.approvalValid &&
          receipt.record.sharingAllowed &&
          !receipt.record.cancelRequested &&
          saved.approvalValid &&
          saved.sharingAllowed &&
          !saved.cancelRequested &&
          this.now() < saved.workUntil,
      );
    } catch {
      return false;
    }
  }

  async supervisorReturn(
    context: AdapterContext & { botId: string },
    run: { id: string; threadId: string },
  ) {
    const receipt = await this.source.forRun(run.id);
    if (
      receipt?.runAccess !== "read" ||
      receipt.record.stage !== "review" ||
      !receipt.record.supervisor ||
      receipt.record.returnTo.botId !== context.botId ||
      receipt.record.returnTo.threadId !== run.threadId
    )
      return undefined;
    const id = piNativeStageId(receipt.record);
    if (!(await this.authorizeResult(id, context, run))) refuse();
    const record = await this.store.read(id);
    if (
      !record ||
      piStageIdentity(record) !== piStageIdentity(receipt.record) ||
      record.state !== "completed" ||
      !record.stopConfirmed ||
      !record.original ||
      record.cancelRequested ||
      record.reconciliationExpired
    )
      refuse();
    return { id, record, returnTo: record.supervisor! };
  }

  /** Read-only authority: target stage and reader Run must still belong to the
   * same current finite consent. Role/thread/hash checks remain in the provider.
   */
  async authorizeResult(id: string, context: AdapterContext, run: { id: string }) {
    try {
      const receipt = await this.source.forRun(run.id);
      const target = await this.store.read(id);
      if (!receipt || !target || !(await this.currentStage(id))) return false;
      const reader = receipt.record;
      const current = await this.source.forStage(piNativeStageId(reader));
      return Boolean(
        current &&
          piStageIdentity(current.record) === piStageIdentity(reader) &&
          current.decisionId === receipt.decisionId &&
          current.approvedBy === context.userId &&
          current.scope === "generalized-task-and-results" &&
          current.record.approvalValid &&
          current.record.sharingAllowed &&
          !current.record.cancelRequested &&
          receipt.decisionId === target.approvalRef &&
          receipt.decisionId === reader.approvalRef &&
          receipt.approvedBy === context.userId &&
          receipt.scope === "generalized-task-and-results" &&
          reader.workId === target.workId &&
          reader.ownerId === context.userId &&
          reader.spaceId === context.spaceId &&
          target.ownerId === context.userId &&
          target.spaceId === context.spaceId &&
          reader.approvalValid &&
          reader.sharingAllowed &&
          !reader.cancelRequested &&
          this.now() < reader.workUntil,
      );
    } catch {
      return false;
    }
  }

  async prepare(
    context: AdapterContext & { botId: string },
    run: { id: string; threadId: string },
    request: Omit<CloudAgentLaunchRequest, "idempotencyKey">,
  ) {
    const approval = await this.forRun(context, run);
    if (
      !approval ||
      request.prompt !== approval.record.input ||
      request.repository ||
      request.openPr ||
      request.images?.length
    )
      refuse();
    const saved = await this.store.read(approval.id);
    if (saved) {
      if (
        piStageIdentity(saved) !== piStageIdentity(approval.record) ||
        !saved.approvalValid ||
        !saved.sharingAllowed ||
        saved.cancelRequested ||
        saved.state === "unknown" ||
        saved.state === "cancelled"
      )
        refuse();
    } else {
      if (this.now() >= approval.record.startUntil) refuse();
      // Receipt records must describe an unstarted reservation. The persistent
      // registrar enforces this and immutable work/stage/handle uniqueness.
      await this.store.bindTrustedStage(approval.id, approval.record);
    }
    return approval;
  }

  async recheck(
    context: AdapterContext & { botId: string },
    run: { id: string; threadId: string },
    expected: { id: string; record: PiStageRecord },
  ) {
    const current = await this.forRun(context, run);
    const saved = await this.store.read(expected.id);
    if (!(await this.currentStage(expected.id))) refuse();
    if (
      !current ||
      current.id !== expected.id ||
      piStageIdentity(current.record) !== piStageIdentity(expected.record) ||
      !saved ||
      piStageIdentity(saved) !== piStageIdentity(expected.record) ||
      !saved.approvalValid ||
      !saved.sharingAllowed ||
      saved.cancelRequested ||
      saved.state === "unknown" ||
      saved.state === "cancelled" ||
      (!saved.dispatched && this.now() >= saved.startUntil)
    )
      refuse();
  }
}

/** Must durably reject mutation of an existing work/stage decision or reassignment
 * of a Run, and reject reuse of the same decision/stage for another work. A production implementation is still required, not an in-memory map.
 */
export interface PiWorkApprovalRegistry extends PiWorkApprovalSource {
  register(runId: string, approval: PiApprovedStage): Promise<void>;
}

/** Internal structured owner-decision entry point. authenticateOwner must be
 * supplied by authenticated request middleware, never by a Bot/MCP identity.
 * It is deliberately not registered as a tool or live HTTP endpoint.
 */
export async function registerPiOwnerStage(
  deps: {
    prisma: PrismaClient;
    approvals: PiWorkApprovalRegistry;
    authenticateOwner(): Promise<{ userId: string; spaceId: string } | undefined>;
    runnerNamespace: string;
    now?: () => number;
  },
  command: { approve: true; runId: string; decisionId: string; record: PiStageRecord },
) {
  const owner = await deps.authenticateOwner().catch(refuse);
  const now = (deps.now ?? Date.now)();
  const record = structuredClone(command.record);
  if (
    !owner ||
    command.approve !== true ||
    !command.decisionId ||
    !["design", "review"].includes(record.stage) ||
    record.ownerId !== owner.userId ||
    record.spaceId !== owner.spaceId ||
    record.approvalRef !== command.decisionId ||
    record.runnerNamespace !== deps.runnerNamespace ||
    !record.workId ||
    !record.originRunId ||
    !record.input ||
    record.state !== "reserved" ||
    record.dispatched ||
    record.original ||
    record.cancelRequested ||
    record.stopConfirmed ||
    record.dispatchClaim ||
    record.dispatchGeneration !== 0 ||
    !record.approvalValid ||
    !record.sharingAllowed ||
    !Number.isFinite(record.startUntil) ||
    !Number.isFinite(record.workUntil) ||
    !Number.isFinite(record.stopReconcileUntil) ||
    record.startUntil <= now ||
    record.startUntil > record.workUntil ||
    record.workUntil > now + 600_000 ||
    record.stopReconcileUntil < record.workUntil ||
    record.stopReconcileUntil > record.workUntil + 300_000
  )
    refuse();
  const [origin, assigned, member, recipient] = await Promise.all([
    deps.prisma.run.findFirst({
      where: { id: record.originRunId, userId: owner.userId, spaceId: owner.spaceId },
      select: { id: true },
    }),
    deps.prisma.run.findFirst({
      where: {
        id: command.runId,
        userId: owner.userId,
        spaceId: owner.spaceId,
        botId: record.botId,
        threadId: record.threadId,
      },
      select: { id: true },
    }),
    deps.prisma.spaceMember.findUnique({
      where: {
        spaceId_userId: {
          spaceId: owner.spaceId,
          userId: owner.userId,
        },
      },
      select: { id: true },
    }),
    deps.prisma.bot.findFirst({
      where: {
        id: record.returnTo.botId,
        userId: owner.userId,
        spaceId: owner.spaceId,
        archivedAt: null,
        thread: { id: record.returnTo.threadId },
      },
      select: { id: true },
    }),
  ]).catch(refuse);
  if (!origin || !assigned || !member || !recipient) refuse();
  await deps.approvals
    .register(command.runId, {
      decisionId: command.decisionId,
      approvedBy: owner.userId,
      scope: "generalized-task-and-results",
      record,
    })
    .catch(refuse);
  return { workId: record.workId, stage: record.stage, id: piNativeStageId(record) };
}
