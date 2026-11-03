// @vitest-environment jsdom
import type { ReactNode } from "react";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

const api = vi.hoisted(() => ({ list: vi.fn(), get: vi.fn(), preview: vi.fn(), approve: vi.fn() }));
vi.mock("../lib/rpc", () => ({
  selectedSpaceId: () => "space",
  createSpaceRpc: () => ({
    bots: { list: api.list },
    threads: { get: api.get },
    piWork: { preview: api.preview, approve: api.approve },
  }),
}));
vi.mock("@lingui/react/macro", () => {
  const t = (s: TemplateStringsArray, ...v: unknown[]) =>
    s.reduce((a, b, i) => a + b + (v[i] ?? ""), "");
  return { useLingui: () => ({ t }), Trans: ({ children }: { children: ReactNode }) => children };
});
vi.mock("@rakazo/ui-web", () => ({
  Button: (p: any) => <button {...p} />,
  NativeSelect: (p: any) => <select {...p} />,
  NativeSelectOption: (p: any) => <option {...p} />,
  Textarea: (p: any) => <textarea {...p} />,
}));

import { PiWorkPage } from "./PiWork";

let root: Root;
const draft = {
  input: "Generalized task",
  design: { botId: "design", threadId: "d" },
  review: { botId: "review", threadId: "r" },
  supervisor: { botId: "supervisor", threadId: "s", requestMessageId: "request" },
  modelId: "provider/model",
  sourcePin: "a".repeat(64),
  workUntil: Date.now() + 600000,
  limits: {
    maxInferences: 10,
    maxOutputTokens: 1024,
    maxPiMs: 120000,
    maxWorkMs: 600000,
    budgetGuidelineUsd: 1,
  },
};
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  api.list.mockResolvedValue([
    { id: "design", name: "Designer" },
    { id: "review", name: "Reviewer" },
    { id: "supervisor", name: "Supervisor" },
  ]);
  api.get.mockResolvedValue({
    messages: [
      {
        id: "original",
        role: "user",
        runId: "origin",
        blocks: [{ kind: "text", text: "Private source text" }],
      },
    ],
  });
  api.preview.mockResolvedValue({
    draft,
    draftHash: "hash",
    returnTarget: { kind: "supervisor_bot" },
  });
  api.approve.mockResolvedValue({ queuedRunId: "queued" });
});
afterEach(() => {
  act(() => root?.unmount());
  document.body.innerHTML = "";
  vi.clearAllMocks();
});
async function open() {
  const host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
  await act(async () =>
    root.render(
      <MemoryRouter initialEntries={["/app/pi-work?botId=design&originRunId=origin"]}>
        <PiWorkPage />
      </MemoryRouter>,
    ),
  );
}
async function change(id: string, value: string) {
  await act(async () => {
    const el = document.getElementById(id)! as HTMLInputElement;
    const proto =
      el.tagName === "TEXTAREA" ? HTMLTextAreaElement.prototype : HTMLSelectElement.prototype;
    Object.getOwnPropertyDescriptor(proto, "value")!.set!.call(el, value);
    el.dispatchEvent(new Event(el.tagName === "TEXTAREA" ? "input" : "change", { bubbles: true }));
  });
}
async function click(text: string) {
  await act(async () => {
    const b = [...document.querySelectorAll("button")].find((b) => b.textContent === text)!;
    b.click();
  });
}
async function preview() {
  await change("pi-review", "review");
  await change("pi-task", "Generalized task");
  await click("Review plan");
}
it("keeps source text out of the shared task and requires explicit approval of the exact preview", async () => {
  await open();
  expect((document.getElementById("pi-task") as HTMLTextAreaElement).value).toBe("");
  expect(api.approve).not.toHaveBeenCalled();
  await preview();
  expect(api.preview).toHaveBeenCalledWith({
    originRunId: "origin",
    reviewBotId: "review",
    input: "Generalized task",
  });
  expect(api.approve).not.toHaveBeenCalled();
  expect(document.body.textContent).toContain("Designer, Reviewer, Supervisor");
  await click("Approve and start");
  await click("Approve and start");
  expect(api.approve).toHaveBeenCalledTimes(1);
  expect(api.approve).toHaveBeenCalledWith({
    draft,
    draftHash: "hash",
    approve: true,
    start: true,
  });
  expect(document.body.textContent).toContain("Approved and queued.");
});
it("invalidates the preview on edits", async () => {
  await open();
  await preview();
  await change("pi-task", "Changed task");
  expect(document.body.textContent).not.toContain("Approve and start");
  expect(api.approve).not.toHaveBeenCalled();
});
it("does not automatically retry an uncertain approval", async () => {
  api.approve.mockRejectedValueOnce(new Error("uncertain"));
  await open();
  await preview();
  await click("Approve and start");
  expect(api.approve).toHaveBeenCalledTimes(1);
  expect(document.body.textContent).toContain("Approval was not confirmed");
  expect(document.body.textContent).not.toContain("Approve and start");
});
