# Pi cloud-agent candidate

This local candidate connects the existing Cloud Agent launch/poll path to the Pi runtime. It is not deployed or live-validated. API and worker must use identical scope, namespace, source pin and model connection references. The original owner must approve a finite work draft and the ordinary launch/read/message gates remain in force.

## Explicit startup references

The default remains disabled. The API and worker share the same parser and composition when `CLOUD_AGENT_PROVIDER=pi`. `CLOUD_AGENT_SPACE_ID` remains the existing space binding. The optional generic `CLOUD_AGENT_CONFIG` supplies missing non-secret references; it does not duplicate model credentials or create a connection:

```text
CLOUD_AGENT_PROVIDER=pi
CLOUD_AGENT_SPACE_ID=<space-id>
CLOUD_AGENT_CONFIG={"ownerId":"<owner-user-id>","namespace":"<reviewed-runner-namespace>","sourcePin":"<reviewed-64-character-source-hash>","model":{"provider":"<provider-id>","id":"<model-id>","connectionId":"<existing-connection-id>"}}
```

These are placeholders, not an applied configuration. Omit `connectionId` only to use the standard resolver's existing selection policy. The provider/model/explicit connection reference are part of the finite approval identity. Invalid explicit Pi configuration stops boot; non-Pi providers do not parse it. Secret fields are rejected. No settings, access grants, connections or credentials are created at startup.

The shared bootstrap uses the application's existing PostgreSQL pool and installs admission/return handlers in both the executor and background polling path. It resolves models lazily through the existing `executor.resolveConnectedModel`, under the stage owner's tenant. Connection validation, credential loading, OAuth and hosted-credit observers retain the existing behavior. No model resolution or provider call occurs during bootstrap. At execution, the fixed model/connection is checked, tools/history are empty, and any stricter connection output limit is retained within the approved 1024-token cap. The ten-call and time bounds remain unchanged.

## Original requester and return scope

A user-origin request can return original design/review results to its original human thread. A bot-origin request uses existing paired request messages and sender Run records to identify its original supervisor. The owner draft binds the exact supervisor Bot, fixed thread and request-message identity along with design/review actors. Current request records, membership, sharing and finite consent are checked again before use. Preview metadata alone is never send authority.

After review returns to design, the saved review reference may be returned to that approved supervisor through the ordinary `message_bot` gate. Both name and fixed current thread must match. Transaction-time authorization and a synchronous final deadline check precede commit. The paired sender/receiver records admit the supervisor to both saved originals through the normal `cloud_agent_result` gate, read-only. It cannot launch Pi, add work or forward that reference onward. Bot prose is never delivery or acceptance evidence; originals retain `accepted=false`.

## Owner confirmation and explicit start

The existing signed-in web route `/app/pi-work` uses the normal space RPC/session client. Select the original request and review Bot, then explicitly enter a generalized task; history is not copied into the shared input. Preview shows exact shared content, participants, return scope, model, limits and budget guideline. Only an explicit “Approve and start” sends the exact draft/hash with owner consent. A server-provided link can preselect an eligible original Run; it contains no task text or credentials. A fresh preview of already-approved work preserves the original deadline/hash.

`start=true` registers consent and creates at most one ordinary queued user Run/Task with a server-recorded activation nonce. Run lineage restores the same private finite receipt; a missing or revoked grant cannot fall back to an unbound execution. The ordinary continuation job and all existing tool gates remain required. Completed/failed original Runs may activate; running/waiting Runs return `startBlocked` without answering an unrelated pending question. Follow the original chat in that case. Network/commit uncertainty is not automatically retried by the page. Activation is an explicit user action, not a model claim or an approval-card bypass.

## Validation and remaining work

Offline tests cover shared API/worker composition, credential/funding selection using the real resolver with fake stores, observer retention, background-job dependency forwarding, scope/connection mismatch rejection, and paired requester inspection. Worker startup itself is only statically checked because importing its entry point starts services. A disposable network-isolated test also ran normal createApp and Vite, password/session-cookie authentication, owner preview and signout against real PostgreSQL using temporary synthetic credentials. It verified that preview creates no finite consent, CloudAgent, extra Run or model credential. Those temporary resources were removed. The worker main, real provider credentials/inference, payments and live settings were not exercised.

A live acceptance run still needs the intended startup references, review of the running build/source pin and connection scope, validation of the existing authenticated confirmation UI, and explicit authorization for model use. A source-pin string is a trusted deployment reference, not binary attestation. The dollar budget is a guideline, not a hard billing guarantee. Recovery must retain UNKNOWN and never rerun an ambiguous Pi execution.

Supervisor handoff and activation now also have isolated real Prisma/PostgreSQL validation: first concurrent activations, first concurrent supervisor sends, transaction rollback on expiry and scoped original reads. The web form remains jsdom-only; HTTP asset delivery does not prove browser rendering. Authentication callbacks, leases, action selection, runtime and queue in the earlier vertical DB harness are synthetic. The separate normal HTTP authentication/preview test does not exercise real Bot inference or the full execution sequence. No claim is made that a real model autonomously selects the complete sequence, that a supervisor made an acceptance decision, or that the complete live flow has passed. The confirmation page is route-accessible and linked by the unbound Pi launch response; no global navigation item is added. Mobile/native confirmation UI remains outside this candidate.

The isolated DB run found a stale pre-lock activation snapshot rejection. The fix compares immutable consent while allowing the already-committed server activation to be validated and reused. A fake regression preserves that snapshot case; actual PostgreSQL first concurrent activation checks pass. Recreating an adapter instance is not an OS process restart, and discarding a successful caller reply is not a lost PostgreSQL COMMIT response.
