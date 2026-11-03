import { createHash } from "node:crypto";
import { createRouterClient } from "@orpc/server";
import { PrismaPiWorkApprovalSource } from "@rakazo/adapters";
import type { Actor } from "@rakazo/contracts";
import { currentDbContext, runAsTenant } from "@rakazo/db";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { PiStageRecord } from "../../../packages/adapters/src/pi-cloud-agent.js";
import type { RouterDeps } from "./router.js";
import { createRouter } from "./router.js";

type Row = Record<string, any>;
const actor: Actor = {
  userId: "owner",
  spaceId: "space",
  email: "owner@example.invalid",
  isDeploymentOwner: false,
};
function fixture() {
  const effects = new Map<string, Row>();
  const records = new Map<string, PiStageRecord>();
  let member = true,
    archived = false;
  const queries = vi.fn(() => expect(currentDbContext()).toMatchObject({ kind: "tenant" }));
  const bots: Row[] = [
    {
      id: "design",
      userId: "owner",
      spaceId: "space",
      archivedAt: null,
      thread: { id: "design-thread" },
    },
    {
      id: "review",
      userId: "owner",
      spaceId: "space",
      archivedAt: null,
      thread: { id: "review-thread" },
    },
  ];
  const matches = (r: Row, w: Row) => Object.entries(w).every(([k, v]) => r[k] === v);
  const prisma: Row = {
    run: {
      findFirst: async ({ where }: Row) => {
        queries();
        const r = {
          id: "origin",
          userId: "owner",
          spaceId: "space",
          botId: "design",
          threadId: "design-thread",
        };
        return matches(r, where) ? r : null;
      },
    },
    spaceMember: {
      findUnique: async () => {
        queries();
        return member ? { id: "member" } : null;
      },
    },
    bot: {
      findFirst: async ({ where }: Row) => {
        queries();
        return (
          bots.find((r) =>
            matches({ ...r, archivedAt: r.id === "review" && archived ? new Date() : null }, where),
          ) ?? null
        );
      },
    },
    thread: {
      findFirst: async ({ where }: Row) => {
        queries();
        return (
          bots
            .map((r) => ({ id: r.thread.id, botId: r.id, userId: r.userId, spaceId: r.spaceId }))
            .find((r) => matches(r, where)) ?? null
        );
      },
    },
    externalEffect: {
      findMany: async ({ where }: Row) => {
        queries();
        return [...effects.values()].filter(
          (r) =>
            r.kind === where.kind &&
            r.status === where.status &&
            r.spaceId === where.spaceId &&
            where.OR.some((q: Row) => r.request.stageIds[q.request.path[1]] === q.request.equals),
        );
      },
      findUnique: async ({ where }: Row) => {
        queries();
        return structuredClone(effects.get(where.id) ?? null);
      },
      create: async ({ data }: Row) => {
        queries();
        if (effects.has(data.id)) throw Error("duplicate");
        effects.set(data.id, structuredClone(data));
        return data;
      },
      updateMany: async ({ where, data }: Row) => {
        queries();
        const rows = [...effects.values()].filter((r) => matches(r, where));
        for (const r of rows) Object.assign(r, data);
        return { count: rows.length };
      },
    },
    $queryRaw: async () => {
      queries();
      return [{ id: "thread" }];
    },
    $transaction: async (fn: (tx: Row) => unknown) => fn(prisma),
  };
  const onStageRead = vi.fn(() => {});
  const stagesForActor = vi.fn(() => ({
    read: async (id: string) => {
      onStageRead();
      return structuredClone(records.get(id));
    },
  }));
  const jobs = { enqueue: vi.fn(async () => undefined) };
  const deps = {
    prisma,
    jobs,
    piWork: {
      runnerNamespace: "fake-runner",
      sourcePin: "a".repeat(64),
      modelId: "fake-model",
      stagesForActor,
    },
  } as unknown as RouterDeps;
  const router = createRouter(deps),
    authorized = vi.fn(async () => true);
  const client = (context: Row = { actor, sessionId: "session", stillAuthorized: authorized }) =>
    createRouterClient(router, { context: context as any });
  const preview = async () => {
    const { draft, draftHash } = await client().piWork.preview({
      originRunId: "origin",
      reviewBotId: "review",
      input: "  Generalized task\n",
    });
    return { draft, draftHash };
  };
  const seedResults = async () =>
    runAsTenant(actor, async () => {
      const source = new PrismaPiWorkApprovalSource(prisma as never, stagesForActor(), {
        userId: "owner",
        spaceId: "space",
        runnerNamespace: "fake-runner",
        sourcePin: "a".repeat(64),
        modelId: "fake-model",
      });
      const receipt = await source.forRun("origin");
      if (!receipt) throw Error("missing fixture receipt");
      const row = effects.get(receipt.decisionId)!;
      const save = (r: PiStageRecord, text: string) => ({
        ...r,
        dispatched: true,
        dispatchGeneration: 1,
        state: "completed" as const,
        stopConfirmed: true,
        original: {
          text,
          hash: createHash("sha256").update(text).digest("hex"),
          ...(r.targetHash ? { targetHash: r.targetHash } : {}),
        },
      });
      records.set(row.request.stageIds.design, save(receipt.record, "Original design\n"));
      const review = await source.forStage(row.request.stageIds.review);
      if (!review) throw Error("missing review fixture receipt");
      records.set(row.request.stageIds.review, save(review.record, "Original review\n"));
    });
  return {
    effects,
    records,
    seedResults,
    onStageRead,
    queries,
    jobs,
    client,
    preview,
    authorized,
    stagesForActor,
    setMember: () => {
      member = false;
    },
    setArchived: () => {
      archived = true;
    },
  };
}
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});
describe("finite Pi owner session RPC", () => {
  it("is unavailable without explicit server composition", async () => {
    const rpc = createRouterClient(createRouter({} as RouterDeps), { context: { actor } });
    await expect(
      rpc.piWork.preview({ originRunId: "origin", reviewBotId: "review", input: "task" }),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
  });
  it.each([
    null,
    { actor },
    { actor, sessionId: "self-asserted" },
    { actor, stillAuthorized: async () => true },
  ])("rejects missing genuine session context: %j", async (context) => {
    const f = fixture();
    await expect(
      f
        .client(context ?? { actor: null })
        .piWork.preview({ originRunId: "origin", reviewBotId: "review", input: "task" }),
    ).rejects.toMatchObject({ code: "UNAUTHORIZED" });
    expect(f.effects.size).toBe(0);
    expect(f.queries).not.toHaveBeenCalled();
  });
  it("rechecks current authorization before preview", async () => {
    const f = fixture();
    f.authorized.mockResolvedValue(false);
    await expect(f.preview()).rejects.toMatchObject({ code: "UNAUTHORIZED" });
    expect(f.queries).not.toHaveBeenCalled();
  });
  it("derives owned participants and stores consent only", async () => {
    const f = fixture();
    const p = await f.preview();
    expect(p.draft).toMatchObject({
      ownerId: "owner",
      spaceId: "space",
      input: "Generalized task",
      design: { botId: "design", threadId: "design-thread" },
      review: { botId: "review", threadId: "review-thread" },
      runnerNamespace: "fake-runner",
      modelId: "fake-model",
    });
    const c = { ...p, approve: true as const };
    const r = await f.client().piWork.approve(c);
    expect(r.workId).toBe("pi-work:origin");
    await f.client().piWork.approve(c);
    expect(f.effects.size).toBe(1);
    expect(f.effects.get(r.decisionId)?.request.approvedByUserId).toBe("owner");
    expect(f.stagesForActor).toHaveBeenCalledWith(actor);
    expect(f.jobs.enqueue).not.toHaveBeenCalled();
  });
  it.each(["ownerId", "spaceId", "modelId", "input"])(
    "rejects edited preview field %s",
    async (field) => {
      const f = fixture();
      const p = await f.preview();
      await expect(
        f.client().piWork.approve({
          ...p,
          approve: true,
          draft: { ...(p.draft as Row), [field]: "changed" },
        }),
      ).rejects.toMatchObject({ code: "CONFLICT" });
      expect(f.effects.size).toBe(0);
    },
  );
  it("rechecks session at actual registration boundary", async () => {
    const f = fixture();
    const p = await f.preview();
    f.authorized.mockReset().mockResolvedValueOnce(true).mockResolvedValueOnce(false);
    await expect(f.client().piWork.approve({ ...p, approve: true })).rejects.toMatchObject({
      code: "CONFLICT",
    });
    expect(f.effects.size).toBe(0);
  });
  it("refuses current membership loss and archived review", async () => {
    const f = fixture();
    const p = await f.preview();
    f.setMember();
    await expect(f.client().piWork.approve({ ...p, approve: true })).rejects.toMatchObject({
      code: "CONFLICT",
    });
    expect(f.effects.size).toBe(0);
    f.setArchived();
    await expect(f.preview()).rejects.toMatchObject({ code: "NOT_FOUND" });
  });
  it("does not treat ordinary OK as an owner decision", async () => {
    const f = fixture();
    await expect(
      f.client().piWork.approve({ draft: "OK", draftHash: "a".repeat(64), approve: true }),
    ).rejects.toMatchObject({ code: "CONFLICT" });
    expect(f.effects.size).toBe(0);
  });
  it("revokes without claiming process stop or waking worker", async () => {
    const f = fixture();
    const p = await f.preview();
    await f.client().piWork.approve({ ...p, approve: true });
    expect(await f.client().piWork.revoke({ originRunId: "origin" })).toEqual({
      ok: true,
      stopConfirmed: false,
    });
    expect(f.effects.get("pi-work:origin")?.status).toBe("pi-work-revoked");
    await expect(f.client().piWork.approve({ ...p, approve: true })).rejects.toMatchObject({
      code: "CONFLICT",
    });
    expect(f.jobs.enqueue).not.toHaveBeenCalled();
  });
});

describe("owner original-result readback", () => {
  async function completed() {
    const f = fixture();
    const p = await f.preview();
    await f.client().piWork.approve({ ...p, approve: true });
    await f.seedResults();
    return f;
  }
  it("returns saved originals to the original owner/thread, without claiming acceptance or sending", async () => {
    const f = await completed();
    const result = await f.client().piWork.result({ originRunId: "origin" });
    expect(result).toMatchObject({
      originRunId: "origin",
      threadId: "design-thread",
      accepted: false,
      original: { design: { text: "Original design\n" }, review: { text: "Original review\n" } },
    });
    expect(result.original.review.targetHash).toBe(result.original.design.hash);
    expect(f.jobs.enqueue).not.toHaveBeenCalled();
  });
  it("does not return partial results when review is missing", async () => {
    const f = await completed();
    f.records.delete(f.effects.get("pi-work:origin")!.request.stageIds.review);
    await expect(f.client().piWork.result({ originRunId: "origin" })).rejects.toMatchObject({
      code: "CONFLICT",
    });
  });
  it.each(["unknown", "cancelled", "target", "bytes", "stop"])(
    "refuses %s evidence",
    async (kind) => {
      const f = await completed();
      const review = f.records.get(f.effects.get("pi-work:origin")!.request.stageIds.review)!;
      if (kind === "unknown" || kind === "cancelled") review.state = kind;
      else if (kind === "target") review.targetHash = "f".repeat(64);
      else if (kind === "bytes") review.original!.text = "edited";
      else review.stopConfirmed = false;
      await expect(f.client().piWork.result({ originRunId: "origin" })).rejects.toMatchObject({
        code: "CONFLICT",
      });
    },
  );
  it("refuses revocation and membership loss without body fallback", async () => {
    const f = await completed();
    await f.client().piWork.revoke({ originRunId: "origin" });
    await expect(f.client().piWork.result({ originRunId: "origin" })).rejects.toMatchObject({
      code: "CONFLICT",
    });
    const other = await completed();
    other.setMember();
    await expect(other.client().piWork.result({ originRunId: "origin" })).rejects.toMatchObject({
      code: "CONFLICT",
    });
  });
  it("rechecks session after reading originals", async () => {
    const f = await completed();
    f.authorized
      .mockReset()
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(false);
    await expect(f.client().piWork.result({ originRunId: "origin" })).rejects.toMatchObject({
      code: "CONFLICT",
    });
  });
  it("refuses a review that becomes UNKNOWN during the final session check", async () => {
    const f = await completed();
    const review = f.records.get(f.effects.get("pi-work:origin")!.request.stageIds.review)!;
    let checks = 0;
    f.authorized.mockImplementation(async () => {
      if (++checks === 3) review.state = "unknown";
      return true;
    });
    await expect(f.client().piWork.result({ originRunId: "origin" })).rejects.toMatchObject({
      code: "CONFLICT",
    });
  });
  it("refuses expiry during the final awaited evidence read", async () => {
    vi.useFakeTimers();
    const f = await completed();
    const until = f.effects.get("pi-work:origin")!.request.draft.workUntil;
    let reads = 0;
    f.onStageRead.mockImplementation(() => {
      if (++reads === 7) vi.setSystemTime(until);
    });
    await expect(f.client().piWork.result({ originRunId: "origin" })).rejects.toMatchObject({
      code: "CONFLICT",
    });
    expect(reads).toBe(7);
  });
  it("refuses grant revocation during the last body reread", async () => {
    const f = await completed();
    let checks = 0;
    f.authorized.mockImplementation(async () => {
      if (++checks === 4) f.effects.get("pi-work:origin")!.status = "pi-work-revoked";
      return true;
    });
    await expect(f.client().piWork.result({ originRunId: "origin" })).rejects.toMatchObject({
      code: "CONFLICT",
    });
  });
  it("does not allow another session owner to read the root grant", async () => {
    const f = await completed();
    await expect(
      f
        .client({
          actor: { ...actor, userId: "other" },
          sessionId: "another-session",
          stillAuthorized: async () => true,
        })
        .piWork.result({ originRunId: "origin" }),
    ).rejects.toMatchObject({ code: "CONFLICT" });
  });
});

describe("explicit owner activation dispatch", () => {
  it("uses the existing continuation job only after consent and queued activation", async () => {
    const f = fixture();
    const p = await f.preview();
    const activation = vi
      .spyOn(PrismaPiWorkApprovalSource.prototype, "activateOwnerWork")
      .mockResolvedValue({ runId: "activation", runState: "queued", enqueue: true });
    await expect(
      f.client().piWork.approve({ ...p, approve: true, start: true }),
    ).resolves.toMatchObject({ queuedRunId: "activation", runState: "queued" });
    expect(activation).toHaveBeenCalledTimes(1);
    expect(f.jobs.enqueue).toHaveBeenCalledTimes(1);
  });
  it("does not enqueue or answer a blocked pending Run", async () => {
    const f = fixture();
    const p = await f.preview();
    vi.spyOn(PrismaPiWorkApprovalSource.prototype, "activateOwnerWork").mockResolvedValue({
      blocked: true,
    });
    await expect(
      f.client().piWork.approve({ ...p, approve: true, start: true }),
    ).resolves.toMatchObject({ startBlocked: true });
    expect(f.jobs.enqueue).not.toHaveBeenCalled();
  });
  it("does not retry an uncertain activation failure", async () => {
    const f = fixture();
    const p = await f.preview();
    const activation = vi
      .spyOn(PrismaPiWorkApprovalSource.prototype, "activateOwnerWork")
      .mockRejectedValue(new Error("uncertain"));
    await expect(f.client().piWork.approve({ ...p, approve: true, start: true })).rejects.toThrow();
    expect(activation).toHaveBeenCalledTimes(1);
    expect(f.jobs.enqueue).not.toHaveBeenCalled();
  });
});
