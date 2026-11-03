import type { PrismaClient, ThreadEvents } from "@rakazo/db";
import { appendEventInTransaction, createThreadMessageInTransaction } from "@rakazo/db";
import type { PrismaPiWorkApprovalSource } from "./pi-work-approval.js";

/** Server completion -> original human thread. No model destination/body, actor
 * assertion or message_bot grant is reused as authority for another Bot. */
export class PiSupervisorReturn {
  constructor(
    private readonly deps: {
      prisma: PrismaClient;
      providerKey: string;
      source(db: PrismaClient): PrismaPiWorkApprovalSource;
      events: Pick<ThreadEvents, "notify">;
      now?: () => number;
    },
  ) {}
  async publish(
    agentId: string,
    generation: number,
  ): Promise<{ status: "sent" | "unavailable" | "unknown"; messageId?: string }> {
    try {
      const initial = await this.deps
        .source(this.deps.prisma)
        .completionResult(agentId, generation, this.deps.providerKey);
      if (!initial) return { status: "unavailable" };
      const committed = await this.deps.prisma.$transaction(async (tx) => {
        const thread = await tx.thread.findFirst({
          where: {
            id: initial.recipient.threadId,
            userId: initial.recipient.userId,
            spaceId: initial.recipient.spaceId,
          },
          select: { id: true, spaceId: true, userId: true, botId: true },
        });
        if (!thread?.botId) throw Error("Pi return unavailable.");
        await tx.$queryRaw`SELECT id FROM threads WHERE id=${thread.id} FOR UPDATE`;
        // Revoke/update of this exact grant cannot commit while publishing.
        await tx.$queryRaw`SELECT id FROM external_effects WHERE id=${initial.workId} FOR SHARE`;
        const source = this.deps.source(tx as unknown as PrismaClient);
        const result = await source.completionResult(agentId, generation, this.deps.providerKey);
        if (
          !result ||
          result.threadId !== thread.id ||
          result.workId !== initial.workId ||
          JSON.stringify(result) !== JSON.stringify(initial)
        )
          throw Error("Pi return unavailable.");
        const clientNonce = `pi-supervisor-result:${result.workId}:${result.original.review.hash}`;
        const blocks = [
          {
            kind: "text" as const,
            text: JSON.stringify({
              kind: "pi_original_results",
              workId: result.workId,
              originRunId: result.originRunId,
              threadId: result.threadId,
              accepted: false,
              original: result.original,
            }),
          },
        ];
        const existing = await tx.message.findFirst({
          where: { threadId: thread.id, clientNonce },
        });
        const assertDeadline = () => {
          if ((this.deps.now?.() ?? Date.now()) >= result.validUntil)
            throw Error("Pi return expired.");
        };
        assertDeadline();
        if (existing) {
          if (existing.role !== "bot" || JSON.stringify(existing.blocks) !== JSON.stringify(blocks))
            throw Error("Pi return mismatch.");
          return { messageId: existing.id, threadId: thread.id, seq: undefined };
        }
        const message = await createThreadMessageInTransaction(tx, {
          threadId: thread.id,
          botId: thread.botId,
          role: "bot",
          clientNonce,
          blocks,
        });
        const event = await appendEventInTransaction(tx, {
          spaceId: thread.spaceId,
          threadId: thread.id,
          botId: thread.botId,
          type: "thread.message.created",
          payload: { messageId: message.id, role: "bot", blocks },
        });
        assertDeadline();
        return { messageId: message.id, threadId: thread.id, seq: event.seq };
      });
      if (committed.seq !== undefined)
        await this.deps.events.notify(committed.threadId, committed.seq).catch(() => undefined);
      return { status: "sent", messageId: committed.messageId };
    } catch {
      return { status: "unknown" };
    }
  }
}
