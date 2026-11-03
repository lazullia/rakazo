import type { AgentRunModel } from "@rakazo/adapter-kit";
import type { PrismaClient, ThreadEvents } from "@rakazo/db";
import { runAsTenant } from "@rakazo/db";
import { z } from "zod";
import type { createRunExecutor } from "./executor.js";
import type { PiStageRecord } from "./pi-cloud-agent.js";
import type { PiStageRegistrar } from "./pi-native-admission.js";
import { createPiRuntimeConnection } from "./pi-runtime-connection.js";
import type { PiStageSqlPool } from "./pi-stage-postgres.js";
import { PostgresPiStageStore } from "./pi-stage-postgres.js";

type ConnectedModelResolver = ReturnType<typeof createRunExecutor>["resolveConnectedModel"];
const id = z.string().trim().min(1).max(128);
const configSchema = z
  .object({
    ownerId: id,
    namespace: id,
    sourcePin: z.string().regex(/^[a-f0-9]{64}$/),
    model: z.object({ provider: id, id, connectionId: id.optional() }).strict(),
  })
  .strict();
export type PiRuntimeStartupConfig = z.infer<typeof configSchema> & { spaceId: string };
/** Generic cloud-agent config contains references only, never credentials. Explicit
 * invalid Pi selection stops boot; none/cursor/emulator never parse this field. */
export function piRuntimeConfigFromEnv(
  source: Record<string, string | undefined>,
): PiRuntimeStartupConfig | undefined {
  if (source.CLOUD_AGENT_PROVIDER?.trim().toLowerCase() !== "pi") return undefined;
  try {
    return {
      ...configSchema.parse(JSON.parse(source.CLOUD_AGENT_CONFIG ?? "")),
      spaceId: id.parse(source.CLOUD_AGENT_SPACE_ID),
    };
  } catch {
    throw Error("Pi cloud-agent configuration is unavailable or invalid.");
  }
}
export async function resolvePiRuntimeModel(
  config: PiRuntimeStartupConfig,
  record: Readonly<PiStageRecord>,
  resolve: ConnectedModelResolver,
): Promise<AgentRunModel> {
  if (
    record.ownerId !== config.ownerId ||
    record.spaceId !== config.spaceId ||
    record.runnerNamespace !== config.namespace ||
    record.runtimeProtocol !== "pi-runtime/v1"
  )
    throw Error("Pi model scope is unavailable.");
  return runAsTenant({ userId: config.ownerId, spaceId: config.spaceId }, () =>
    resolve(
      { userId: config.ownerId, spaceId: config.spaceId },
      config.model.provider,
      config.model.id,
      undefined,
      false,
      undefined,
      config.model.connectionId,
    ),
  );
}
/** Shared API/worker boot composition. No model resolution or send at startup. */
export function createPiRuntimeBootstrap(deps: {
  config?: PiRuntimeStartupConfig;
  prisma: PrismaClient;
  pool?: PiStageSqlPool;
  stageStore?: PiStageRegistrar;
  events: Pick<ThreadEvents, "notify">;
  resolveConnectedModel: ConnectedModelResolver;
}) {
  const config = deps.config ? structuredClone(deps.config) : undefined;
  if (!config) return undefined;
  const scope = { userId: config.ownerId, spaceId: config.spaceId };
  const store =
    deps.stageStore ?? (deps.pool ? new PostgresPiStageStore(deps.pool, scope) : undefined);
  if (!store) throw Error("Pi cloud-agent requires its durable stage store.");
  return createPiRuntimeConnection({
    prisma: deps.prisma,
    store,
    events: deps.events,
    scope,
    namespace: config.namespace,
    sourcePin: config.sourcePin,
    model: config.model,
    resolveModel: (record) => resolvePiRuntimeModel(config, record, deps.resolveConnectedModel),
  });
}
