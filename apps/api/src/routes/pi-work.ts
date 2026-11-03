import { ORPCError } from "@orpc/server";
import { runContinueJob } from "@rakazo/adapter-kit";
import type { PiStageStore } from "@rakazo/adapters";
import { inspectPiSupervisor, PrismaPiWorkApprovalSource } from "@rakazo/adapters";
import type { Actor } from "@rakazo/contracts";
import type { RouterContext } from "./context.js";

/** Trusted composition, not request settings. A reader is tenant scoped by the
 * server; namespace/pin/model must match the configured worker before deployment. */
export interface PiOwnerWorkConfig {
  scope?: { userId: string; spaceId: string };
  runnerNamespace: string;
  sourcePin: string;
  modelId: string;
  stagesForActor(actor: Actor): Pick<PiStageStore, "read">;
}

export function piWorkRoutes({ deps, authed }: RouterContext) {
  const ownerWork = authed.use(async ({ context, next }) => {
    if (
      !deps.piWork ||
      (deps.piWork.scope &&
        (deps.piWork.scope.userId !== context.actor.userId ||
          deps.piWork.scope.spaceId !== context.actor.spaceId))
    )
      throw new ORPCError("NOT_FOUND");
    // The ordinary actor middleware also serves non-session internal callers.
    // A finite owner decision must come from the existing genuine session path.
    const authenticateOwner = async () => {
      if (!context.sessionId || !context.stillAuthorized || !(await context.stillAuthorized()))
        throw new ORPCError("UNAUTHORIZED");
      return { userId: context.actor.userId, spaceId: context.actor.spaceId };
    };
    const scope = await authenticateOwner();
    const source = new PrismaPiWorkApprovalSource(
      deps.prisma,
      deps.piWork.stagesForActor(context.actor),
      {
        ...scope,
        runnerNamespace: deps.piWork.runnerNamespace,
        sourcePin: deps.piWork.sourcePin,
        modelId: deps.piWork.modelId,
      },
    );
    return next({ context: { ...context, source, authenticateOwner } });
  });
  return {
    piWork: {
      preview: ownerWork.piWork.preview.handler(async ({ context, input }) => {
        const scope = { spaceId: context.actor.spaceId, userId: context.actor.userId };
        const [origin, review] = await Promise.all([
          deps.prisma.run.findFirst({
            where: { id: input.originRunId, ...scope },
            select: { botId: true, threadId: true },
          }),
          deps.prisma.bot.findFirst({
            where: { id: input.reviewBotId, ...scope, archivedAt: null },
            select: { id: true, thread: { select: { id: true } } },
          }),
        ]);
        if (!origin || !review?.thread) throw new ORPCError("NOT_FOUND");
        try {
          const returnTarget = await inspectPiSupervisor(
            deps.prisma,
            scope,
            input.originRunId,
            input.reviewBotId,
          );
          const proposal = await context.source.previewOwnerWork(
            {
              originRunId: input.originRunId,
              input: input.input,
              design: { botId: origin.botId, threadId: origin.threadId },
              review: { botId: review.id, threadId: review.thread.id },
              ...(returnTarget.kind === "supervisor_bot"
                ? {
                    supervisor: {
                      botId: returnTarget.botId,
                      threadId: returnTarget.threadId,
                      requestMessageId: returnTarget.requestMessageId,
                    },
                  }
                : {}),
            },
            context.authenticateOwner,
          );
          return {
            ...proposal,
            returnTarget,
          };
        } catch {
          throw new ORPCError("BAD_REQUEST", { message: "Pi work preview is unavailable." });
        }
      }),
      approve: ownerWork.piWork.approve.handler(async ({ context, input }) => {
        try {
          // Registration rechecks current ownership/participants and exact bytes.
          // An explicit start queues a normal Run; it does not bypass any tool gate.
          const receipt = await context.source.registerOwnerWork(
            { draft: input.draft, draftHash: input.draftHash, approve: input.approve },
            context.authenticateOwner,
          );
          if (!input.start) return receipt;
          const activation = await context.source.activateOwnerWork(
            receipt.workId,
            receipt.draftHash,
            context.authenticateOwner,
          );
          if ("blocked" in activation) return { ...receipt, startBlocked: true };
          if ("eventSeq" in activation && activation.eventSeq !== undefined)
            await deps.events
              .notify(activation.threadId!, activation.eventSeq)
              .catch(() => undefined);
          if (activation.enqueue)
            await deps.jobs.enqueue(runContinueJob(activation.runId)).catch(() => undefined);
          return {
            ...receipt,
            runId: activation.runId,
            runState: activation.runState,
            ...(activation.runState === "queued" ? { queuedRunId: activation.runId } : {}),
          };
        } catch {
          throw new ORPCError("CONFLICT", {
            message: "Pi work approval is unavailable; readback is required.",
          });
        }
      }),
      result: ownerWork.piWork.result.handler(async ({ context, input }) => {
        try {
          return await context.source.ownerResult(input.originRunId, context.authenticateOwner);
        } catch {
          throw new ORPCError("CONFLICT", {
            message: "Pi work result is unavailable; readback is required.",
          });
        }
      }),
      revoke: ownerWork.piWork.revoke.handler(async ({ context, input }) => {
        try {
          await context.source.revoke(input.originRunId, context.authenticateOwner);
          // Revocation is distinct from runner termination confirmation.
          return { ok: true as const, stopConfirmed: false as const };
        } catch {
          throw new ORPCError("CONFLICT", {
            message: "Pi work revocation is unavailable; readback is required.",
          });
        }
      }),
    },
  };
}
