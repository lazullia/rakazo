import type { AssistantMessage, Model } from "@earendil-works/pi-ai";
import { AssistantMessageEventStream } from "@earendil-works/pi-ai/utils/event-stream";
import { describe, expect, it, vi } from "vitest";

const wire = vi.hoisted(() => ({ stream: vi.fn() }));
vi.mock("@earendil-works/pi-ai/providers/all", () => ({
  builtinModels: () => ({
    getModel: () => ({
      provider: "fake-runtime",
      id: "bounded",
      name: "bounded",
      api: "openai-completions",
      baseUrl: "https://model.example.invalid/v1",
      reasoning: false,
      input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 32768,
      maxTokens: 4096,
    }),
    streamSimple: (...args: unknown[]) => wire.stream(...args),
  }),
}));
vi.mock("./pi-local-provider.js", () => ({ registerLocalProvider: (m: unknown) => m }));
vi.mock("./pi-openai-compatible-provider.js", () => ({
  OPENAI_COMPATIBLE_PROVIDER_ID: "openai-compatible",
  registerOpenAiCompatibleCatalog: (m: unknown) => m,
  registerOpenAiCompatibleRuntime: (m: unknown) => m,
}));

import { binding } from "./pi-cloud-agent.test-fixtures.js";
import { PiRuntimeExecution } from "./pi-runtime-execution.js";

describe("execution through existing PiAgentRuntime", () => {
  it.each(["Synthetic original", ""])(
    "uses real runtime/core, preserving only model text: %j",
    async (text) => {
      wire.stream.mockReset();
      wire.stream.mockImplementation((model: Model<"openai-completions">, _context, options) => {
        expect(options.maxTokens).toBe(1024);
        expect(options.maxRetries).toBe(0);
        const response: AssistantMessage = {
          role: "assistant",
          api: model.api,
          provider: model.provider,
          model: model.id,
          content: text ? [{ type: "text", text }] : [],
          stopReason: "stop",
          timestamp: 1000,
          usage: {
            input: 1,
            output: 2,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 3,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
          },
        };
        const s = new AssistantMessageEventStream();
        s.push({ type: "start", partial: { ...response, content: [] } });
        s.push({ type: "done", reason: "stop", message: response });
        s.end(response);
        return s;
      });
      const beforeCall = vi.fn(async () => "accounted-reservation"),
        afterCall = vi.fn(async () => {});
      const job = new PiRuntimeExecution(binding({ dispatched: true, dispatchGeneration: 1 }), {
        namespace: "fake-pi",
        model: { provider: "fake-runtime", id: "bounded" },
        authorized: async () => true,
        now: () => 1000,
        resolveModel: async () => ({
          provider: "fake-runtime",
          id: "bounded",
          apiKey: "synthetic",
          callObserver: { beforeCall, afterCall },
        }),
      });
      expect(await job.execute()).toEqual(text ? { kind: "completed", text } : { kind: "unknown" });
      expect(wire.stream).toHaveBeenCalledOnce();
      expect(beforeCall).toHaveBeenCalledOnce();
      expect(afterCall).toHaveBeenCalledOnce();
      expect(job.diagnostics()).toMatchObject({ inferences: 1, stopped: true });
    },
  );
});
