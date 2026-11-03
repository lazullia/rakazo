import type { AdapterContext } from "@rakazo/adapter-kit";
import { disabledBuiltinToolSet } from "@rakazo/contracts";
import type { CloudAgentDeps } from "./cloud-agent-service.js";
import { cloudAgentResultSchema } from "./cloud-agent-tools.js";
import type { PiCloudAgentProvider } from "./pi-cloud-agent.js";

const unavailable = () => ({ error: "Original result is unavailable for this run." });

/** Server identity check shared by Pi tools; caller supplies no model identity. */
export async function currentPiRun(
  deps: Pick<CloudAgentDeps, "prisma" | "runLease">,
  context: AdapterContext & { botId: string },
  run: { id: string; threadId: string },
  toolName: string,
) {
  const lease = deps.runLease;
  if (!lease || lease.runId !== run.id) return false;

  const [active, thread, bot, member] = await Promise.all([
    deps.prisma.run.findFirst({
      where: {
        id: run.id,
        userId: context.userId,
        spaceId: context.spaceId,
        botId: context.botId,
        threadId: run.threadId,
        status: "running",
        leaseOwner: lease.owner,
        leaseFence: lease.fence,
        leaseExpiresAt: { gt: new Date() },
      },
      select: { id: true },
    }),
    deps.prisma.thread.findFirst({
      where: {
        id: run.threadId,
        userId: context.userId,
        spaceId: context.spaceId,
        botId: context.botId,
      },
      select: { id: true },
    }),
    deps.prisma.bot.findFirst({
      where: {
        id: context.botId,
        userId: context.userId,
        spaceId: context.spaceId,
        archivedAt: null,
      },
      select: { id: true, disabledBuiltinTools: true },
    }),
    deps.prisma.spaceMember.findUnique({
      where: { spaceId_userId: { spaceId: context.spaceId, userId: context.userId } },
      select: { id: true },
    }),
  ]);
  return Boolean(
    active &&
      thread &&
      bot &&
      member &&
      !disabledBuiltinToolSet(bot.disabledBuiltinTools).has(toolName),
  );
}

/** Called only after the ordinary tool gate. Identity comes from the leased Run. */
export async function readPiCloudAgentResult(
  deps: CloudAgentDeps,
  provider: PiCloudAgentProvider,
  context: AdapterContext & { botId: string },
  run: { id: string; threadId: string },
  args: Record<string, unknown>,
) {
  const input = cloudAgentResultSchema.safeParse(args);
  const lease = deps.runLease;
  if (!input.success || !lease || lease.runId !== run.id) return unavailable();
  const current = async () =>
    Boolean(
      deps.piAdmission &&
        (await currentPiRun(deps, context, run, "cloud_agent_result")) &&
        (await deps.piAdmission.authorizeResult(input.data.id, context, run)),
    );
  try {
    context.signal.throwIfAborted();
    if (!(await current())) return unavailable();
    const result = await provider.readForRun(input.data.id, { ...context, threadId: run.threadId });
    // A paused/reclaimed Run, archived Bot or removed member must not receive bytes
    // read while its membership was changing. No caller-supplied identity is used.
    if (!(await current())) return unavailable();
    context.signal.throwIfAborted();
    return result;
  } catch {
    return unavailable();
  }
}
