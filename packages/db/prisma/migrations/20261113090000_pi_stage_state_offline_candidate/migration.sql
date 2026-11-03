-- Isolated candidate only. Additive: previous releases do not use these tables.
-- One row lock covers both fixed stages and their shared cancellation/approval checks.
CREATE TABLE "pi_work_states" (
  "id" TEXT PRIMARY KEY,
  "ownerId" TEXT NOT NULL,
  "spaceId" TEXT NOT NULL,
  "workId" TEXT NOT NULL,
  "stages" JSONB NOT NULL DEFAULT '{}',
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "pi_work_states_scope_work_key" UNIQUE ("ownerId", "spaceId", "workId"),
  CONSTRAINT "pi_work_states_id_scope_key" UNIQUE ("id", "ownerId", "spaceId"),
  CONSTRAINT "pi_work_states_fixed_stages" CHECK (
    jsonb_typeof("stages") = 'object' AND ("stages" - 'design' - 'review') = '{}'
  )
);

-- Canonical immutable execution identity: one handle per work/stage and one
-- work/stage per handle within the configured runner's stable namespace.
CREATE TABLE "pi_stage_handles" (
  "workStateId" TEXT NOT NULL,
  "ownerId" TEXT NOT NULL,
  "spaceId" TEXT NOT NULL,
  "stage" TEXT NOT NULL CHECK ("stage" IN ('design', 'review')),
  "runnerNamespace" TEXT NOT NULL,
  "handle" TEXT NOT NULL,
  CONSTRAINT "pi_stage_handles_pkey" PRIMARY KEY ("workStateId", "stage"),
  CONSTRAINT "pi_stage_handles_runner_handle_key" UNIQUE ("runnerNamespace", "handle"),
  CONSTRAINT "pi_stage_handles_work_scope_fkey" FOREIGN KEY ("workStateId", "ownerId", "spaceId")
    REFERENCES "pi_work_states" ("id", "ownerId", "spaceId") ON DELETE RESTRICT
);

-- Aliases survive chat/CloudAgent deletion; deleting a chat must not permit relaunch.
CREATE TABLE "pi_stage_bindings" (
  "agentId" TEXT PRIMARY KEY,
  "workStateId" TEXT NOT NULL,
  "ownerId" TEXT NOT NULL,
  "spaceId" TEXT NOT NULL,
  "stage" TEXT NOT NULL CHECK ("stage" IN ('design', 'review')),
  CONSTRAINT "pi_stage_bindings_work_scope_fkey" FOREIGN KEY ("workStateId", "ownerId", "spaceId")
    REFERENCES "pi_work_states" ("id", "ownerId", "spaceId") ON DELETE RESTRICT,
  CONSTRAINT "pi_stage_bindings_handle_fkey" FOREIGN KEY ("workStateId", "stage")
    REFERENCES "pi_stage_handles" ("workStateId", "stage") ON DELETE RESTRICT
);
CREATE INDEX "pi_stage_bindings_workStateId_idx" ON "pi_stage_bindings" ("workStateId");

ALTER TABLE "pi_work_states" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "pi_work_states" FORCE ROW LEVEL SECURITY;
-- Separate operation policies: even a broad legacy GRANT cannot delete a
-- tombstone or update an alias. There is deliberately no DELETE policy.
CREATE POLICY pi_work_states_read ON "pi_work_states" FOR SELECT
  USING ("ownerId" = NULLIF(current_setting('app.user_id', true), '')
    AND "spaceId" = NULLIF(current_setting('app.space_id', true), ''));
CREATE POLICY pi_work_states_insert ON "pi_work_states" FOR INSERT
  WITH CHECK ("ownerId" = NULLIF(current_setting('app.user_id', true), '')
    AND "spaceId" = NULLIF(current_setting('app.space_id', true), ''));
CREATE POLICY pi_work_states_update ON "pi_work_states" FOR UPDATE
  USING ("ownerId" = NULLIF(current_setting('app.user_id', true), '')
    AND "spaceId" = NULLIF(current_setting('app.space_id', true), ''))
  WITH CHECK ("ownerId" = NULLIF(current_setting('app.user_id', true), '')
    AND "spaceId" = NULLIF(current_setting('app.space_id', true), ''));

ALTER TABLE "pi_stage_bindings" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "pi_stage_bindings" FORCE ROW LEVEL SECURITY;
CREATE POLICY pi_stage_bindings_read ON "pi_stage_bindings" FOR SELECT
  USING ("ownerId" = NULLIF(current_setting('app.user_id', true), '')
    AND "spaceId" = NULLIF(current_setting('app.space_id', true), ''));
CREATE POLICY pi_stage_bindings_insert ON "pi_stage_bindings" FOR INSERT
  WITH CHECK ("ownerId" = NULLIF(current_setting('app.user_id', true), '')
    AND "spaceId" = NULLIF(current_setting('app.space_id', true), ''));

ALTER TABLE "pi_stage_handles" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "pi_stage_handles" FORCE ROW LEVEL SECURITY;
CREATE POLICY pi_stage_handles_read ON "pi_stage_handles" FOR SELECT
  USING ("ownerId" = NULLIF(current_setting('app.user_id', true), '')
    AND "spaceId" = NULLIF(current_setting('app.space_id', true), ''));
CREATE POLICY pi_stage_handles_insert ON "pi_stage_handles" FOR INSERT
  WITH CHECK ("ownerId" = NULLIF(current_setting('app.user_id', true), '')
    AND "spaceId" = NULLIF(current_setting('app.space_id', true), ''));
