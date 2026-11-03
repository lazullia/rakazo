import { createHash } from "node:crypto";
import type { AgentRunModel, AgentRuntime, ModelCallObserver } from "@rakazo/adapter-kit";
import type { PrismaClient, ThreadEvents } from "@rakazo/db";
import type { PiStageRecord } from "./pi-cloud-agent.js";
import { PiCloudAgentProvider } from "./pi-cloud-agent.js";
import type { PiStageRegistrar } from "./pi-native-admission.js";
import { PiNativeAdmission } from "./pi-native-admission.js";
import { PiRunApprovalSource } from "./pi-run-approval-source.js";
import { PiRuntimeRunner } from "./pi-runtime-runner.js";
import { PiSupervisorReturn } from "./pi-supervisor-return.js";
import { PrismaPiWorkApprovalSource } from "./pi-work-approval.js";

export function piRuntimeModelIdentity(model: {
  provider: string;
  id: string;
  connectionId?: string;
}) {
  return `${encodeURIComponent(model.provider)}/${encodeURIComponent(model.id)}${model.connectionId ? `@${encodeURIComponent(model.connectionId)}` : ""}`;
}

/** Explicit trusted composition; never an environment/HTTP/MCP actor switch.
 * API and worker must use identical scope/source/model settings and durable store. */
export function createPiRuntimeConnection(input: {
  prisma: PrismaClient;
  store: PiStageRegistrar;
  events: Pick<ThreadEvents, "notify">;
  scope: { userId: string; spaceId: string };
  namespace: string;
  sourcePin: string;
  model: { provider: string; id: string; connectionId?: string };
  resolveModel(record: Readonly<PiStageRecord>): Promise<AgentRunModel>;
  createRuntime?: (observer: ModelCallObserver) => Promise<AgentRuntime>;
  now?: () => number;
}) {
  const deps = { ...input, scope: { ...input.scope }, model: { ...input.model } };
  const scope = deps.scope;
  const key = `pi-runtime:${createHash("sha256")
    .update(
      JSON.stringify([
        deps.scope.userId,
        deps.scope.spaceId,
        deps.namespace,
        deps.sourcePin,
        deps.model.provider,
        deps.model.id,
        deps.model.connectionId ?? null,
      ]),
    )
    .digest("hex")}`;
  const config = {
    ...deps.scope,
    runnerNamespace: deps.namespace,
    sourcePin: deps.sourcePin,
    // The approved model identity includes its provider, not only an SDK-local ID.
    modelId: piRuntimeModelIdentity(deps.model),
  };
  const sourceFor = (db: PrismaClient) =>
    new PrismaPiWorkApprovalSource(db, deps.store, config, deps.now);
  const authority = sourceFor(deps.prisma);
  const source = new PiRunApprovalSource(deps.prisma, authority, deps.store, key, deps.now);
  const piAdmission = new PiNativeAdmission(source, deps.store, deps.now);
  const runner = new PiRuntimeRunner(deps.store, deps.namespace, {
    model: deps.model,
    authorized: (id) => piAdmission.currentStage(id),
    resolveModel: deps.resolveModel,
    createRuntime: deps.createRuntime,
    now: deps.now,
  });
  const provider = new PiCloudAgentProvider(deps.store, runner, deps.now);
  const piSupervisorReturn = new PiSupervisorReturn({
    prisma: deps.prisma,
    providerKey: key,
    source: sourceFor,
    events: deps.events,
    now: deps.now,
  });
  return {
    cloudAgent: { provider, key, spaceId: deps.scope.spaceId },
    piAdmission,
    piSupervisorReturn,
    authority,
    runner,
    piWork: {
      scope,
      runnerNamespace: config.runnerNamespace,
      sourcePin: config.sourcePin,
      modelId: config.modelId,
      stagesForActor: (actor: { userId: string; spaceId: string }) => {
        if (actor.userId !== scope.userId || actor.spaceId !== scope.spaceId)
          throw Error("Pi owner scope unavailable.");
        return deps.store;
      },
    },
  };
}
