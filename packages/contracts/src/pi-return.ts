import { z } from "zod";
/** An inspected route is not permission to share or send a result. */
export const PiReturnCandidateSchema = z.discriminatedUnion("kind", [
  z.object({
    kind: z.literal("owner_thread"),
    threadId: z.string(),
    needsAdditionalApproval: z.literal(false),
  }),
  z.object({
    kind: z.literal("supervisor_bot"),
    botId: z.string(),
    threadId: z.string(),
    requestMessageId: z.string(),
    needsAdditionalApproval: z.literal(true),
  }),
  z.object({ kind: z.literal("unavailable"), needsAdditionalApproval: z.literal(true) }),
]);
export type PiReturnCandidate = z.infer<typeof PiReturnCandidateSchema>;
