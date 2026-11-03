// Opt-in subprocess fixture: exits only after a real DB dispatch-intent commit.

import type { PiStageRecord, PiStageRunner } from "./pi-cloud-agent.js";
import { PiCloudAgentProvider } from "./pi-cloud-agent.js";
import { context } from "./pi-cloud-agent.test-fixtures.js";
import { PostgresPiStageStore } from "./pi-stage-postgres.js";
import { IsolatedPsqlPool } from "./pi-stage-postgres.test-db.js";

const input = JSON.parse(process.argv[2] ?? "null") as { agentId: string; record: PiStageRecord };
if (!process.env.PI_STAGE_TEST_DESCRIPTOR || !input?.agentId)
  throw new Error("Isolated crash fixture only");
const store = new PostgresPiStageStore(new IsolatedPsqlPool(), {
  userId: "owner",
  spaceId: "space",
});
await store.bindTrustedStage(input.agentId, input.record);
const runner: PiStageRunner = {
  namespace: "fake-pi",
  startReserved: async () => {
    process.exit(23);
  },
  lookup: async () => ({ kind: "missing" }),
  cancel: async () => ({ kind: "unknown" }),
};
await new PiCloudAgentProvider(store, runner, () => 1).launch(
  { idempotencyKey: input.agentId, prompt: input.record.input },
  context,
);
throw new Error("Crash fixture did not exit at the dispatch boundary");
