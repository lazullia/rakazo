import { Trans, useLingui } from "@lingui/react/macro";
import type { Bot, ThreadMessage } from "@rakazo/contracts";
import { Button, NativeSelect, NativeSelectOption, Textarea } from "@rakazo/ui-web";
import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { z } from "zod";
import { createSpaceRpc, selectedSpaceId } from "../lib/rpc";

const actor = z.object({ botId: z.string(), threadId: z.string() });
const viewSchema = z.object({
  input: z.string(),
  design: actor,
  review: actor,
  supervisor: actor.extend({ requestMessageId: z.string() }).optional(),
  modelId: z.string(),
  sourcePin: z.string(),
  workUntil: z.number(),
  limits: z.object({
    maxInferences: z.number(),
    maxOutputTokens: z.number(),
    maxPiMs: z.number(),
    maxWorkMs: z.number(),
    budgetGuidelineUsd: z.number(),
  }),
});
type Preview = { draft: unknown; draftHash: string; view: z.infer<typeof viewSchema> };
/** Existing signed-in RPC/session client; no token entry or model-side consent. */
export function PiWorkPage() {
  const { t } = useLingui();
  const [params] = useSearchParams();
  const rpc = useMemo(() => createSpaceRpc(selectedSpaceId()), []);
  const [bots, setBots] = useState<Bot[]>([]),
    [design, setDesign] = useState(params.get("botId") ?? ""),
    [review, setReview] = useState(""),
    [requests, setRequests] = useState<ThreadMessage[]>([]),
    [origin, setOrigin] = useState(""),
    [task, setTask] = useState("");
  const [preview, setPreview] = useState<Preview>(),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [saved, setSaved] = useState<"queued" | "approved">();
  const inFlight = useRef(false);
  useEffect(() => {
    let active = true;
    void rpc.bots
      .list()
      .then((rows) => {
        if (active) setBots(rows);
      })
      .catch(() => {
        if (active) setError(t`Delegation is unavailable.`);
      });
    return () => {
      active = false;
    };
  }, [rpc, t]);
  useEffect(() => {
    let active = true;
    setOrigin("");
    setRequests([]);
    if (design)
      void rpc.threads
        .get({ botId: design })
        .then((thread) => {
          if (active)
            setRequests(thread.messages.filter((m) => m.role === "user" && Boolean(m.runId)));
          if (
            active &&
            thread.messages.some((m) => m.role === "user" && m.runId === params.get("originRunId"))
          )
            setOrigin(params.get("originRunId")!);
        })
        .catch(() => {
          if (active) setError(t`Could not load requests.`);
        });
    return () => {
      active = false;
    };
  }, [design, params, rpc, t]);
  useEffect(() => {
    setPreview(undefined);
    setSaved(undefined);
    setError("");
  }, [design, review, origin, task]);
  const name = (id: string) => bots.find((b) => b.id === id)?.name ?? id;
  async function prepare() {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    setSaved(undefined);
    setError("");
    try {
      const p = await rpc.piWork.preview({ originRunId: origin, reviewBotId: review, input: task });
      const draft = structuredClone(p.draft);
      setPreview({ draft, draftHash: p.draftHash, view: viewSchema.parse(draft) });
    } catch {
      setError(t`Could not prepare this delegation.`);
    } finally {
      setBusy(false);
      inFlight.current = false;
    }
  }
  async function approve() {
    if (!preview || inFlight.current || saved) return;
    inFlight.current = true;
    setBusy(true);
    setError("");
    try {
      const r = await rpc.piWork.approve({
        draft: preview.draft,
        draftHash: preview.draftHash,
        approve: true,
        start: true,
      });
      setSaved(r.queuedRunId ? "queued" : "approved");
    } catch {
      setPreview(undefined);
      setError(t`Approval was not confirmed. Check the original chat before trying again.`);
    } finally {
      setBusy(false);
      inFlight.current = false;
    }
  }
  return (
    <main className="min-h-full bg-background px-6 py-10 text-foreground">
      <div className="mx-auto max-w-xl space-y-5">
        <Link
          to={design ? `/app/${encodeURIComponent(design)}` : "/app"}
          className="text-sm underline"
        >
          <Trans>Back to chat</Trans>
        </Link>
        <h1 className="text-xl font-semibold">
          <Trans>Design and review</Trans>
        </h1>
        <div className="space-y-3">
          <label htmlFor="pi-design" className="block text-sm">
            <Trans>Design Bot</Trans>
            <NativeSelect
              id="pi-design"
              aria-label={t`Design Bot`}
              value={design}
              disabled={busy}
              onChange={(e) => setDesign(e.target.value)}
              className="mt-1 w-full"
            >
              <NativeSelectOption value="">{t`Choose a Bot`}</NativeSelectOption>
              {bots.map((b) => (
                <NativeSelectOption key={b.id} value={b.id}>
                  {b.name}
                </NativeSelectOption>
              ))}
            </NativeSelect>
          </label>
          <label htmlFor="pi-review" className="block text-sm">
            <Trans>Review Bot</Trans>
            <NativeSelect
              id="pi-review"
              aria-label={t`Review Bot`}
              value={review}
              disabled={busy}
              onChange={(e) => setReview(e.target.value)}
              className="mt-1 w-full"
            >
              <NativeSelectOption value="">{t`Choose a Bot`}</NativeSelectOption>
              {bots
                .filter((b) => b.id !== design)
                .map((b) => (
                  <NativeSelectOption key={b.id} value={b.id}>
                    {b.name}
                  </NativeSelectOption>
                ))}
            </NativeSelect>
          </label>
          <label htmlFor="pi-request" className="block text-sm">
            <Trans>Original request</Trans>
            <NativeSelect
              id="pi-request"
              aria-label={t`Original request`}
              value={origin}
              disabled={busy}
              onChange={(e) => setOrigin(e.target.value)}
              className="mt-1 w-full"
            >
              <NativeSelectOption value="">{t`Choose a request`}</NativeSelectOption>
              {requests.map((m) => (
                <NativeSelectOption key={m.id} value={m.runId!}>
                  {m.blocks
                    .map((b) =>
                      b.kind === "text"
                        ? b.text
                        : b.kind === "bot_message_received"
                          ? b.fromBotName
                          : "",
                    )
                    .join(" ")
                    .slice(0, 100)}
                </NativeSelectOption>
              ))}
            </NativeSelect>
          </label>
          <label htmlFor="pi-task" className="block text-sm">
            <Trans>Generalized task to share</Trans>
            <Textarea
              id="pi-task"
              aria-label={t`Generalized task to share`}
              value={task}
              maxLength={16000}
              disabled={busy}
              onChange={(e) => setTask(e.target.value)}
              className="mt-1 min-h-28"
            />
          </label>
          <Button
            disabled={busy || !design || !review || design === review || !origin || !task.trim()}
            onClick={() => void prepare()}
          >
            <Trans>Review plan</Trans>
          </Button>
        </div>
        {preview ? (
          <section
            className="space-y-3 rounded-lg border border-border p-4"
            aria-label={t`Delegation plan`}
          >
            <p className="whitespace-pre-wrap text-sm">{preview.view.input}</p>
            <dl className="space-y-2 text-sm">
              <div>
                <dt className="text-muted-foreground">
                  <Trans>Shared with</Trans>
                </dt>
                <dd>
                  {[
                    preview.view.design.botId,
                    preview.view.review.botId,
                    ...(preview.view.supervisor ? [preview.view.supervisor.botId] : []),
                  ]
                    .map(name)
                    .join(", ")}{" "}
                  · {preview.view.modelId}
                </dd>
              </div>
              <div>
                <dt className="text-muted-foreground">
                  <Trans>Return to</Trans>
                </dt>
                <dd>
                  {preview.view.supervisor ? name(preview.view.supervisor.botId) : t`Original chat`}
                </dd>
              </div>
              <div>
                <dt className="text-muted-foreground">
                  <Trans>Limits</Trans>
                </dt>
                <dd>{t`Each Pi: ${preview.view.limits.maxInferences} calls, ${preview.view.limits.maxOutputTokens} output tokens, ${preview.view.limits.maxPiMs / 1000} seconds. Total: ${preview.view.limits.maxWorkMs / 60000} minutes.`}</dd>
              </div>
            </dl>
            <p className="text-sm text-muted-foreground">{t`Budget guideline: $${preview.view.limits.budgetGuidelineUsd}; not a guaranteed billing cap. Share no secrets or private material. Unconfirmed stop blocks further work. Existing tool approvals still apply.`}</p>
            <details className="text-xs">
              <summary>
                <Trans>Approval details</Trans>
              </summary>
              <pre className="overflow-auto whitespace-pre-wrap">
                {JSON.stringify({ draft: preview.draft, hash: preview.draftHash }, null, 2)}
              </pre>
            </details>
            <Button
              disabled={busy || Boolean(saved) || Date.now() >= preview.view.workUntil}
              onClick={() => void approve()}
            >
              <Trans>Approve and start</Trans>
            </Button>
          </section>
        ) : null}
        {saved ? (
          <p role="status" className="text-sm">
            {saved === "queued"
              ? t`Approved and queued. Follow progress in the original chat.`
              : t`Approval saved. Continue in the original chat.`}
          </p>
        ) : null}
        {error ? (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        ) : null}
      </div>
    </main>
  );
}
