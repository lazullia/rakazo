import { currentDbContext } from "@rakazo/db";
import { describe, expect, it, vi } from "vitest";
import { createBackgroundJobHandlers } from "./background-job-handlers.js";
import { pollCloudAgent } from "./cloud-agent-poll.js";
import { binding, FakeStageStore } from "./pi-cloud-agent.test-fixtures.js";
import {
  createPiRuntimeBootstrap,
  piRuntimeConfigFromEnv,
  resolvePiRuntimeModel,
} from "./pi-runtime-bootstrap.js";
import { piRuntimeModelIdentity } from "./pi-runtime-connection.js";

const raw = {
  ownerId: "owner",
  namespace: "pi-runtime/v1",
  sourcePin: "a".repeat(64),
  model: { provider: "fake", id: "bounded", connectionId: "owned-connection" },
};
const config = { ...raw, spaceId: "space" };
describe("explicit Pi API/worker startup", () => {
  it.each([undefined, "none", "emulator", "cursor"])(
    "does not parse or resolve Pi settings for %s",
    (provider) => {
      expect(
        piRuntimeConfigFromEnv({ CLOUD_AGENT_PROVIDER: provider, CLOUD_AGENT_CONFIG: "not-json" }),
      ).toBeUndefined();
      const resolve = vi.fn();
      expect(
        createPiRuntimeBootstrap({
          prisma: {} as never,
          events: {} as never,
          resolveConnectedModel: resolve,
        }),
      ).toBeUndefined();
      expect(resolve).not.toHaveBeenCalled();
    },
  );
  it("parses references only; unknown credential fields and missing scope fail closed without echo", () => {
    expect(
      piRuntimeConfigFromEnv({
        CLOUD_AGENT_PROVIDER: "pi",
        CLOUD_AGENT_SPACE_ID: "space",
        CLOUD_AGENT_CONFIG: JSON.stringify(raw),
      }),
    ).toEqual(config);
    for (const input of [
      "not-json",
      JSON.stringify({ ...raw, apiKey: "synthetic-private-value" }),
      JSON.stringify({ ...raw, model: { ...raw.model, apiKey: "synthetic-private-value" } }),
    ]) {
      try {
        piRuntimeConfigFromEnv({
          CLOUD_AGENT_PROVIDER: "pi",
          CLOUD_AGENT_SPACE_ID: "space",
          CLOUD_AGENT_CONFIG: input,
        });
        throw Error("expected refusal");
      } catch (e) {
        expect(String(e)).toContain("configuration is unavailable");
        expect(String(e)).not.toContain("synthetic-private-value");
      }
    }
    expect(() =>
      piRuntimeConfigFromEnv({
        CLOUD_AGENT_PROVIDER: "pi",
        CLOUD_AGENT_CONFIG: JSON.stringify(raw),
      }),
    ).toThrow();
  });
  it("requires the durable store for an explicit Pi selection", () => {
    expect(() =>
      createPiRuntimeBootstrap({
        config,
        prisma: {} as never,
        events: {} as never,
        resolveConnectedModel: vi.fn(),
      }),
    ).toThrow("durable stage store");
  });
  it("constructs both boot paths with stable identity without resolving credentials or sending", () => {
    const records = new FakeStageStore();
    const store = {
      read: records.read.bind(records),
      change: records.change.bind(records),
      bindTrustedStage: async () => {},
    };
    const resolve = vi.fn();
    const deps = {
      config,
      prisma: {} as never,
      stageStore: store,
      events: { notify: vi.fn() },
      resolveConnectedModel: resolve,
    };
    const api = createPiRuntimeBootstrap(deps)!,
      worker = createPiRuntimeBootstrap(deps)!;
    expect(api.cloudAgent.key).toBe(worker.cloudAgent.key);
    expect(api.piWork.scope).toEqual({ userId: "owner", spaceId: "space" });
    expect(api.piWork.modelId).toBe("fake/bounded@owned-connection");
    expect(() => api.piWork.stagesForActor({ userId: "other", spaceId: "space" })).toThrow();
    expect(resolve).not.toHaveBeenCalled();
  });
  it("uses the standard connected model resolver under the stage tenant, preserving credits", async () => {
    const observer = { beforeCall: vi.fn(), afterCall: vi.fn() };
    const resolve = vi.fn(async () => {
      expect(currentDbContext()).toMatchObject({
        kind: "tenant",
        userId: "owner",
        spaceId: "space",
      });
      return {
        provider: "fake",
        id: "bounded",
        connectionId: "owned-connection",
        apiKey: "synthetic",
        callObserver: observer,
      };
    });
    const model = await resolvePiRuntimeModel(
      config,
      binding({ runtimeProtocol: "pi-runtime/v1", runnerNamespace: config.namespace }),
      resolve,
    );
    expect(model.callObserver).toBe(observer);
    expect(resolve).toHaveBeenCalledWith(
      { userId: "owner", spaceId: "space" },
      "fake",
      "bounded",
      undefined,
      false,
      undefined,
      "owned-connection",
    );
    expect(observer.beforeCall).not.toHaveBeenCalled();
  });
  it("refuses foreign stage identity before secret resolution", async () => {
    const resolve = vi.fn();
    await expect(
      resolvePiRuntimeModel(
        config,
        binding({
          ownerId: "other",
          runtimeProtocol: "pi-runtime/v1",
          runnerNamespace: config.namespace,
        }),
        resolve,
      ),
    ).rejects.toThrow("scope");
    expect(resolve).not.toHaveBeenCalled();
  });
  it("keeps connection identity distinct and unambiguous", () => {
    expect(piRuntimeModelIdentity({ provider: "a", id: "b@c" })).not.toBe(
      piRuntimeModelIdentity({ provider: "a", id: "b", connectionId: "c" }),
    );
    expect(piRuntimeModelIdentity({ provider: "a", id: "b", connectionId: "one" })).not.toBe(
      piRuntimeModelIdentity({ provider: "a", id: "b", connectionId: "two" }),
    );
  });
});

vi.mock("./cloud-agent-poll.js", () => ({ pollCloudAgent: vi.fn(async () => undefined) }));
describe("normal background job composition", () => {
  it("retains Pi admission and final return inside the job owner's tenant", async () => {
    const piAdmission = {} as never,
      piSupervisorReturn = {} as never;
    const deps = {
      prisma: { cloudAgent: { findUnique: async () => ({ userId: "owner", spaceId: "space" }) } },
      jobs: {},
      events: {},
      cloudAgent: {} as never,
      piAdmission,
      piSupervisorReturn,
    };
    vi.mocked(pollCloudAgent).mockImplementationOnce(async (input) => {
      expect(input.piAdmission).toBe(piAdmission);
      expect(input.piSupervisorReturn).toBe(piSupervisorReturn);
      expect(currentDbContext()).toMatchObject({
        kind: "tenant",
        userId: "owner",
        spaceId: "space",
      });
    });
    const handlers = createBackgroundJobHandlers(deps as never);
    await handlers["cloud_agent.poll"]({ agentId: "stage" });
    expect(pollCloudAgent).toHaveBeenCalledOnce();
  });
});
