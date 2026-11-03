import type { MessageBlock, PiReturnCandidate } from "@rakazo/contracts";
import type { PrismaClient } from "@rakazo/db";

const unavailable = (): PiReturnCandidate => ({
  kind: "unavailable",
  needsAdditionalApproval: true,
});
const block = (value: unknown) =>
  Array.isArray(value) && value.length === 1 ? (value[0] as MessageBlock) : undefined;
/** Address inspection only. Restore the original server-paired request rather than
 * a model's claimed supervisor/name, and never turn this route into send consent. */
export async function inspectPiSupervisor(
  prisma: PrismaClient,
  scope: { userId: string; spaceId: string },
  originRunId: string,
  reviewBotId: string,
): Promise<PiReturnCandidate> {
  try {
    const root = await prisma.run.findFirst({ where: { id: originRunId, ...scope } });
    if (!root?.sourceMessageId) return unavailable();
    const inbound = await prisma.message.findFirst({
      where: { id: root.sourceMessageId, threadId: root.threadId, role: "user", runId: root.id },
    });
    if (!inbound) return unavailable();
    if (root.trigger === "user")
      return { kind: "owner_thread", threadId: root.threadId, needsAdditionalApproval: false };
    if (root.trigger !== "bot_message") return unavailable();
    const received = block(inbound.blocks);
    if (
      received?.kind !== "bot_message_received" ||
      received.intent !== "request" ||
      !received.returnToMessageId ||
      received.fromBotId === root.botId ||
      received.fromBotId === reviewBotId
    )
      return unavailable();
    const supervisor = await prisma.bot.findFirst({
      where: { id: received.fromBotId, ...scope, archivedAt: null },
      select: { id: true, thread: { select: { id: true } } },
    });
    if (!supervisor?.thread) return unavailable();
    const outbound = await prisma.message.findFirst({
      where: {
        id: received.returnToMessageId,
        threadId: supervisor.thread.id,
        botId: supervisor.id,
        role: "bot",
      },
    });
    const sent = block(outbound?.blocks);
    if (
      !outbound?.runId ||
      sent?.kind !== "bot_message_sent" ||
      sent.intent !== "request" ||
      sent.toBotId !== root.botId ||
      sent.text !== received.text
    )
      return unavailable();
    const sender = await prisma.run.findFirst({
      where: { id: outbound.runId, ...scope, botId: supervisor.id, threadId: supervisor.thread.id },
    });
    if (!sender) return unavailable();
    return {
      kind: "supervisor_bot",
      botId: supervisor.id,
      threadId: supervisor.thread.id,
      requestMessageId: outbound.id,
      needsAdditionalApproval: true,
    };
  } catch {
    return unavailable();
  }
}
