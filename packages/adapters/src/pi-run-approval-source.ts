import { createHash } from "node:crypto";
import type { MessageBlock } from "@rakazo/contracts";
import type { PrismaClient, Run } from "@rakazo/db";
import type { PiStageStore } from "./pi-cloud-agent.js";
import type { PiApprovedStage, PiWorkApprovalSource } from "./pi-native-admission.js";
import { piNativeStageId, piResultReferenceText, piStageIdentity } from "./pi-native-admission.js";

type BoundRun = Pick<
  Run,
  "id" | "userId" | "spaceId" | "botId" | "threadId" | "trigger" | "clientNonce" | "sourceMessageId"
>;
const unavailable = () => new Error("Pi Run lineage is unavailable.");
function requireValue<T>(value: T | null | undefined): T {
  if (!value) throw unavailable();
  return value;
}
function block(blocks: unknown): MessageBlock | undefined {
  return Array.isArray(blocks) && blocks.length === 1 ? (blocks[0] as MessageBlock) : undefined;
}

/** Resolve only existing native wake/delivery evidence. No Run, receipt, job or
 * approval is created here. The underlying authority must authenticate/persist
 * initial owner decisions and immutable stage receipts; this does not replace it.
 */
export class PiRunApprovalSource implements PiWorkApprovalSource {
  constructor(
    private readonly prisma: PrismaClient,
    private readonly authority: PiWorkApprovalSource,
    private readonly stages: Pick<PiStageStore, "read">,
    private readonly providerKey: string,
    private readonly now: () => number = Date.now,
  ) {}

  async forStage(id: string) {
    try {
      const receipt = await this.authority.forStage(id);
      if (receipt) await this.root(receipt);
      return receipt;
    } catch {
      throw unavailable();
    }
  }

  private async root(receipt: PiApprovedStage) {
    const origin = requireValue(await this.run(receipt.record.originRunId));
    const root = requireValue(await this.authority.forRun(origin.id));
    this.actor(root, origin);
    if (
      root.decisionId !== receipt.decisionId ||
      root.record.workId !== receipt.record.workId ||
      root.record.ownerId !== receipt.record.ownerId ||
      root.record.spaceId !== receipt.record.spaceId ||
      root.record.originRunId !== receipt.record.originRunId
    )
      throw unavailable();
  }

  private run(id: string) {
    return this.prisma.run.findFirst({
      where: { id },
      select: {
        id: true,
        userId: true,
        spaceId: true,
        botId: true,
        threadId: true,
        trigger: true,
        clientNonce: true,
        sourceMessageId: true,
      },
    });
  }

  private live(receipt: PiApprovedStage, run: BoundRun) {
    const record = receipt.record;
    if (
      receipt.approvedBy !== run.userId ||
      receipt.decisionId !== record.approvalRef ||
      receipt.scope !== "generalized-task-and-results" ||
      record.ownerId !== run.userId ||
      record.spaceId !== run.spaceId ||
      !record.approvalValid ||
      !record.sharingAllowed ||
      record.cancelRequested ||
      !(this.now() < record.workUntil)
    )
      throw unavailable();
  }

  private actor(receipt: PiApprovedStage, run: BoundRun) {
    this.live(receipt, run);
    if (
      receipt.record.botId !== run.botId ||
      receipt.record.threadId !== run.threadId ||
      (receipt.runAccess !== undefined && receipt.runAccess !== "execute")
    )
      throw unavailable();
  }

  private async execution(run: BoundRun): Promise<PiApprovedStage | undefined> {
    // Only initial authenticated registrations may be resolved by this index.
    // Missing child mappings below are recovered from server-created evidence.
    const initial = await this.authority.forRun(run.id);
    if (initial) {
      await this.root(initial);
      this.actor(initial, run);
      return { ...initial, runAccess: "execute" };
    }
    if (run.trigger !== "cloud_agent") return undefined;
    const match = /^cloud-agent-wake-run:(pi-[a-f0-9]{64}):(0|[1-9][0-9]*)$/.exec(
      run.clientNonce ?? "",
    );
    if (!match || run.sourceMessageId || !Number.isSafeInteger(Number(match[2])))
      throw unavailable();
    const id = match[1]!;
    const generation = Number(match[2]);
    const agent = requireValue(
      await this.prisma.cloudAgent.findFirst({
        where: {
          id,
          providerKey: this.providerKey,
          userId: run.userId,
          spaceId: run.spaceId,
          botId: run.botId,
          threadId: run.threadId,
        },
      }),
    );
    if (
      agent.generation !== generation ||
      agent.wakeGeneration !== generation ||
      agent.status !== "finished" ||
      agent.cancelRequested ||
      agent.remoteId !== id
    )
      throw unavailable();
    const receipt = requireValue(await this.forStage(id));
    if (piNativeStageId(receipt.record) !== id) throw unavailable();
    this.actor(receipt, run);
    return { ...receipt, runAccess: "execute" };
  }

  private async delivery(
    run: BoundRun,
    requireDesign = false,
  ): Promise<PiApprovedStage | undefined> {
    if (run.trigger !== "bot_message" || !run.sourceMessageId) return undefined;
    const inbound = requireValue(
      await this.prisma.message.findFirst({
        where: {
          id: run.sourceMessageId,
          threadId: run.threadId,
        },
      }),
    );
    if (!inbound.clientNonce?.startsWith("bot-message:pi-result:")) return undefined;
    const match = /^bot-message:pi-result:(pi-[a-f0-9]{64}):([a-f0-9]{64})$/.exec(
      inbound.clientNonce,
    );
    if (!match || inbound.role !== "user" || inbound.runId !== run.id) throw unavailable();
    const id = match[1]!;
    const hash = match[2]!;
    const receipt = requireValue(await this.forStage(id));
    this.live(receipt, run);
    const saved = requireValue(await this.stages.read(id));
    if (
      (requireDesign && saved.stage !== "design") ||
      piNativeStageId(receipt.record) !== id ||
      piStageIdentity(receipt.record) !== piStageIdentity(saved) ||
      !saved.approvalValid ||
      !saved.sharingAllowed ||
      saved.cancelRequested ||
      saved.state !== "completed" ||
      !saved.stopConfirmed ||
      !saved.original ||
      saved.original.hash !== hash ||
      createHash("sha256").update(saved.original.text).digest("hex") !== hash ||
      saved.returnTo.botId !== run.botId ||
      saved.returnTo.threadId !== run.threadId
    )
      throw unavailable();
    const received = block(inbound.blocks);
    if (
      received?.kind !== "bot_message_received" ||
      received.fromBotId !== saved.botId ||
      received.intent !== "result" ||
      !received.returnToMessageId
    )
      throw unavailable();
    const outbound = requireValue(
      await this.prisma.message.findFirst({
        where: {
          id: received.returnToMessageId,
          threadId: saved.threadId,
        },
      }),
    );
    const sent = block(outbound.blocks);
    const expected = piResultReferenceText(id, saved.stage, hash);
    if (
      outbound.role !== "bot" ||
      !outbound.runId ||
      sent?.kind !== "bot_message_sent" ||
      sent.toBotId !== run.botId ||
      sent.intent !== "result" ||
      sent.text !== expected ||
      received.text !== expected
    )
      throw unavailable();
    const sender = requireValue(await this.run(outbound.runId));
    const execution = await this.execution(sender);
    // A review may finish in the Run that launched it. Admit exactly the
    // review -> verified design delivery -> initial/native design ancestry.
    // The inner design path cannot enter this branch, so cycles cannot recurse.
    const senderReceipt = requireValue(
      execution ??
        (!requireDesign && saved.stage === "review" && sender.trigger === "bot_message"
          ? await this.delivery(sender, true)
          : undefined),
    );
    this.actor(senderReceipt, sender);
    if (
      piNativeStageId(senderReceipt.record) !== id ||
      piStageIdentity(senderReceipt.record) !== piStageIdentity(saved)
    )
      throw unavailable();

    if (saved.stage === "review") return { ...receipt, runAccess: "read" };
    // Review authority must already be derived from the approved finite plan;
    // peer text/receipt of a design does not itself authorize starting a review.
    const review = await this.forStage(piNativeStageId({ ...saved, stage: "review" }));
    if (!review) return { ...receipt, runAccess: "read" };
    this.live(review, run);
    const next = review.record;
    if (
      review.decisionId !== receipt.decisionId ||
      next.workId !== saved.workId ||
      next.botId !== run.botId ||
      next.threadId !== run.threadId ||
      next.originRunId !== saved.originRunId ||
      next.targetHash !== hash ||
      next.returnTo.botId !== saved.botId ||
      next.returnTo.threadId !== saved.threadId
    )
      throw unavailable();
    return { ...review, runAccess: "execute" };
  }

  private async supervisorDelivery(run: BoundRun): Promise<PiApprovedStage | undefined> {
    if (run.trigger !== "bot_message" || !run.sourceMessageId) return undefined;
    const inbound = requireValue(
      await this.prisma.message.findFirst({
        where: { id: run.sourceMessageId, threadId: run.threadId },
      }),
    );
    if (!inbound.clientNonce?.startsWith("bot-message:pi-supervisor:")) return undefined;
    const match = /^bot-message:pi-supervisor:(pi-[a-f0-9]{64}):([a-f0-9]{64})$/.exec(
      inbound.clientNonce,
    );
    if (!match || inbound.role !== "user" || inbound.runId !== run.id) throw unavailable();
    const receipt = requireValue(await this.forStage(match[1]!));
    this.live(receipt, run);
    const saved = requireValue(await this.stages.read(match[1]!));
    if (
      saved.stage !== "review" ||
      piStageIdentity(saved) !== piStageIdentity(receipt.record) ||
      saved.supervisor?.botId !== run.botId ||
      saved.supervisor.threadId !== run.threadId ||
      saved.state !== "completed" ||
      !saved.stopConfirmed ||
      !saved.original ||
      saved.original.hash !== match[2] ||
      saved.original.targetHash !== saved.targetHash ||
      createHash("sha256").update(saved.original.text).digest("hex") !== match[2] ||
      saved.cancelRequested ||
      saved.reconciliationExpired ||
      !saved.approvalValid ||
      !saved.sharingAllowed
    )
      throw unavailable();
    const received = block(inbound.blocks);
    if (
      received?.kind !== "bot_message_received" ||
      received.fromBotId !== saved.returnTo.botId ||
      received.intent !== "result" ||
      !received.returnToMessageId
    )
      throw unavailable();
    const outbound = requireValue(
      await this.prisma.message.findFirst({
        where: { id: received.returnToMessageId, threadId: saved.returnTo.threadId },
      }),
    );
    const sent = block(outbound.blocks);
    const text = piResultReferenceText(match[1]!, "review", match[2]!);
    if (
      outbound.role !== "bot" ||
      outbound.botId !== saved.returnTo.botId ||
      !outbound.runId ||
      sent?.kind !== "bot_message_sent" ||
      sent.toBotId !== run.botId ||
      sent.intent !== "result" ||
      sent.text !== text ||
      received.text !== text
    )
      throw unavailable();
    const sender = requireValue(await this.run(outbound.runId));
    const senderReceipt = requireValue(await this.delivery(sender));
    this.live(senderReceipt, sender);
    if (
      senderReceipt.runAccess !== "read" ||
      piStageIdentity(senderReceipt.record) !== piStageIdentity(saved) ||
      sender.botId !== saved.returnTo.botId ||
      sender.threadId !== saved.returnTo.threadId
    )
      throw unavailable();
    return { ...receipt, runAccess: "read" };
  }

  async forRun(id: string) {
    try {
      const run = await this.run(id);
      if (!run) return undefined;
      return structuredClone(
        (await this.execution(run)) ??
          (await this.delivery(run)) ??
          (await this.supervisorDelivery(run)),
      );
    } catch {
      throw unavailable();
    }
  }
}
