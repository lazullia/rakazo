import { describe, expect, it } from "vitest";
import { inspectPiSupervisor } from "./pi-supervisor-target.js";

function fixture() {
  const scope = { userId: "owner", spaceId: "space" };
  const root: any = {
    id: "root",
    ...scope,
    botId: "design",
    threadId: "design-thread",
    trigger: "bot_message",
    sourceMessageId: "received",
  };
  const sender: any = {
    id: "sender",
    ...scope,
    botId: "supervisor",
    threadId: "supervisor-thread",
  };
  const inbound: any = {
    id: "received",
    threadId: "design-thread",
    runId: "root",
    role: "user",
    blocks: [
      {
        kind: "bot_message_received",
        fromBotId: "supervisor",
        fromBotName: "untrusted name",
        text: "generalized request",
        intent: "request",
        returnToMessageId: "sent",
      },
    ],
  };
  const outbound: any = {
    id: "sent",
    threadId: "supervisor-thread",
    botId: "supervisor",
    runId: "sender",
    role: "bot",
    blocks: [
      {
        kind: "bot_message_sent",
        toBotId: "design",
        text: "generalized request",
        intent: "request",
      },
    ],
  };
  const supervisor: any = {
    id: "supervisor",
    ...scope,
    archivedAt: null,
    thread: { id: "supervisor-thread" },
  };
  const matches = (r: any, w: any) => Object.entries(w).every(([k, v]) => r[k] === v);
  const prisma: any = {
    run: {
      findFirst: async ({ where }: any) => [root, sender].find((r) => matches(r, where)) ?? null,
    },
    message: {
      findFirst: async ({ where }: any) =>
        [inbound, outbound].find((r) => matches(r, where)) ?? null,
    },
    bot: { findFirst: async ({ where }: any) => (matches(supervisor, where) ? supervisor : null) },
  };
  return {
    scope,
    root,
    sender,
    inbound,
    outbound,
    supervisor,
    inspect: () => inspectPiSupervisor(prisma, scope, "root", "review"),
  };
}
describe("original supervisor address inspection, not send approval", () => {
  it("restores only the current owned server-paired requester and still requires new sharing consent", async () => {
    const f = fixture();
    expect(await f.inspect()).toEqual({
      kind: "supervisor_bot",
      botId: "supervisor",
      threadId: "supervisor-thread",
      requestMessageId: "sent",
      needsAdditionalApproval: true,
    });
  });
  it("binds direct human request to the original thread", async () => {
    const f = fixture();
    f.root.trigger = "user";
    f.inbound.blocks = [{ kind: "text", text: "generalized task" }];
    expect(await f.inspect()).toEqual({
      kind: "owner_thread",
      threadId: "design-thread",
      needsAdditionalApproval: false,
    });
  });
  it.each([
    "sender-scope",
    "recipient",
    "body",
    "intent",
    "thread",
    "inbound-run",
    "archived",
    "missing-run",
  ])("refuses %s mismatch without guessing an alternate target", async (kind) => {
    const f = fixture();
    if (kind === "sender-scope") f.sender.userId = "other";
    if (kind === "recipient") f.outbound.blocks[0].toBotId = "other";
    if (kind === "body") f.outbound.blocks[0].text = "different";
    if (kind === "intent") f.inbound.blocks[0].intent = "fyi";
    if (kind === "thread") f.supervisor.thread.id = "new-thread";
    if (kind === "inbound-run") f.inbound.runId = "other-run";
    if (kind === "archived") f.supervisor.archivedAt = new Date();
    if (kind === "missing-run") f.outbound.runId = "missing";
    expect(await f.inspect()).toEqual({ kind: "unavailable", needsAdditionalApproval: true });
  });
});
