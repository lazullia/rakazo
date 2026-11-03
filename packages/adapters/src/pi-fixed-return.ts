import type { AdapterContext } from "@rakazo/adapter-kit";
import { messageBot } from "./bot-messages.js";
import type { ExecutorDeps } from "./executor-deps.js";
import type { PiCloudAgentProvider } from "./pi-cloud-agent.js";
import { currentPiRun } from "./pi-cloud-agent-result.js";

/** Called only from the ordinary gated message_bot handler. No result prose or
 * destination supplied by a model is used as execution/delivery evidence.
 * Send an original reference; the recipient still needs cloud_agent_result's gate.
 */
export async function returnFixedPiResult(
  deps: ExecutorDeps,
  provider: PiCloudAgentProvider,
  context: AdapterContext & { botId: string },
  run: { id: string; threadId: string; sourceMessageId?: string | null },
  sender: { id: string; name: string },
  args: Record<string, unknown>,
) {
  if (!deps.piAdmission) return undefined;
  try {
    const approval = await deps.piAdmission.forRun(context, run);
    if (!approval) return undefined;
    if (args.intent !== "result")
      return { error: "Use result intent for the approved Pi result return." };
    if (args.bot_id && args.bot_id !== approval.record.returnTo.botId)
      return { error: "Pi result destination differs from the approved recipient." };
    if (!(await currentPiRun(deps, context, run, "message_bot")))
      return { error: "Pi result return is not authorized for this run." };
    const reference = await provider.returnReference(
      approval.id,
      context,
      approval.record.returnTo,
    );
    await deps.piAdmission.recheck(context, run, approval);
    if (!(await currentPiRun(deps, context, run, "message_bot")))
      return { error: "Pi result return is not authorized for this run." };
    const message = `Pi ${reference.stage} original: ${reference.id}; sha256 ${reference.originalHash}. Use cloud_agent_result to read it. Acceptance has not been determined.`;
    if (typeof args.message !== "string" || args.message.trim() !== message)
      return {
        error: "Pi result return message differs from the approved reference.",
        expectedMessage: message,
      };
    let expectedName: string | undefined;
    if (args.confirm_name !== undefined) {
      if (typeof args.confirm_name !== "string" || !args.confirm_name.trim())
        return { error: "Pi result recipient name is invalid." };
      const candidates = await deps.prisma.bot.findMany({
        where: { spaceId: context.spaceId, userId: context.userId, archivedAt: null },
        select: { id: true, name: true },
      });
      const name = args.confirm_name.trim().toLowerCase();
      const matches = candidates.filter((bot) => bot.name.toLowerCase() === name);
      if (matches.length !== 1 || matches[0]!.id !== reference.returnTo.botId)
        return { error: "Pi result recipient name differs from the approved recipient." };
      expectedName = matches[0]!.name;
    }
    const sent = await messageBot(
      deps,
      { ...run, userId: context.userId, spaceId: context.spaceId, botId: context.botId },
      sender,
      {
        bot_id: reference.returnTo.botId,
        intent: "result",
        message: args.message,
        // Work/stage/result identity, independent of effect/Run/retry identity.
        deliveryKey: `pi-result:${reference.id}:${reference.originalHash}`,
      },
      { expectedThreadId: reference.returnTo.threadId, expectedName },
    );
    return sent.ok
      ? { ok: true, botId: sent.botId, name: sent.name, note: sent.note }
      : { error: sent.error };
  } catch {
    return { error: "Pi result return is not authorized for this run." };
  }
}
