# Plan-bound supervised model identity — design summary

**Status:** PENDING OWNER REVIEW

**Date:** 2026-09-01

## Result

Make the supervised subscription rail derive its exact provider/model identity from the approved immutable `ExecutionPlan` instead of the current hard-coded `openai-codex / gpt-5.6-terra` pair.

The first acceptance canary will use the active Hermes configuration discovered read-only on 2026-09-01:

```text
provider_id = minimax-oauth
model_id    = MiniMax-M3
```

The existing canary `job-bca98b65bc214cd8a5d149223b2dc472` remains immutable and blocked in `OPERATOR_RECOVERY_REQUIRED`. Its accepted completions are not relabelled, deleted, replayed, or reused.

## Incident being fixed

The authoritative execution plan already stores ordered `model_ids` and `provider_ids`, but the supervised rail currently:

1. validates the plan against hard-coded Terra constants;
2. writes the same constants into every stage packet;
3. writes those constants into final `model_usage`;
4. accepts operator-provided CLI attestation when it matches the hard-coded packet.

All observed completions in the current canary were actually produced by MiniMax M3, while the first three accepted ledger records were attested as `openai-codex / gpt-5.6-terra`. Finalization is therefore correctly blocked.

## Chosen approach

### Plan-bound single identity

For a supervised subscription run, the approved plan must contain exactly:

- one non-empty `provider_id`;
- one non-empty `model_id`;
- `maximum_retries = 0`;
- the existing supervised pipeline, executor, result destination, and cost-boundary contract.

Plan validation returns a frozen `SupervisedRuntimeIdentity(provider_id, model_id)`. That value is passed explicitly through packet preparation and finalization.

The approved plan fingerprint remains the authority boundary. Changing either identity creates a different plan fingerprint and therefore requires a new approval and a new run.

## Alternatives rejected

### Hard-code MiniMax M3

Rejected. It would repeat the same architectural defect with a different vendor and break again at the next model switch.

### Provider/model allowlist in application configuration

Rejected for this slice. The rail performs no provider I/O; its job is to bind an owner-approved identity to operator-observed output. An additional allowlist would duplicate approval authority without proving which model actually ran.

### Mutate the current ledger

Rejected. Accepted completions and their attestations are immutable provenance records. Rewriting them would destroy the evidence the rail exists to preserve.

## Data flow

```text
ExecutionPlan(model_ids=(M,), provider_ids=(P,))
  -> validate supervised plan and derive identity(P, M)
  -> build outline packet with P/M
  -> persist immutable packet and approval fingerprint
  -> operator executes in visible Hermes session configured as P/M
  -> supervised-bind compares observed attestation to packet P/M
  -> next packets inherit exactly the same P/M
  -> finalizer re-reads approved plan identity
  -> manifest.model_usage records exactly P/M
```

## Code changes

### `src/seo_orchestrator/supervised_rail.py`

- Replace behavior-level dependence on `SUPERVISED_PROVIDER_ID` and `SUPERVISED_MODEL_ID` with a frozen `SupervisedRuntimeIdentity` value.
- Require identity explicitly in `build_stage_packet(...)` and `SupervisedRail.prepare_packet(...)`.
- Preserve provider/model in packet identity and therefore in `input_hash`.
- Keep bind checks exact and fail-closed for session, provider, model, stage, job, company, and input hash.
- Keep existing ledgers readable; no schema migration and no historical rewriting.

### `src/seo_orchestrator/services/supervised_subscription.py`

- Change `_validate_plan(...)` to return the single approved runtime identity rather than compare against Terra constants.
- Reject zero, multiple, or mismatched-length provider/model tuples.
- Pass the identity into all packet construction paths.
- Pass the same identity into `_execution_result(...)` so final `model_usage` is plan-bound.
- Revalidate the plan at finalization so an approval/fingerprint inconsistency fails closed.

### `src/seo_orchestrator/cli.py`

- Preserve explicit operator attestation inputs.
- Do not infer actual session identity from the global Hermes default: a session can be switched independently.
- Ensure operator-facing packet/status output exposes the exact required provider/model pair before bind.

### Tests

Modify only supervised rail/subscription/CLI tests that currently assume Terra constants.

## TDD acceptance tests

1. **RED:** a valid supervised plan with `minimax-oauth / MiniMax-M3` produces an outline packet with exactly that identity.
2. **RED:** zero or multiple model IDs fail before packet creation.
3. **RED:** zero or multiple provider IDs fail before packet creation.
4. **RED:** MiniMax packet rejects a Terra operator attestation without accepting a completion.
5. **RED:** MiniMax packet accepts an exact MiniMax attestation and the next packet preserves the same identity.
6. **RED:** resume with an identity differing from the frozen packet fails closed.
7. **RED:** finalized artifact `model_usage` records `minimax-oauth / MiniMax-M3`, not compile-time constants.
8. **REGRESSION:** recovery-required and cancel semantics remain unchanged.
9. **REGRESSION:** transport/process import guards remain green; no provider SDK or network client is added.
10. **REGRESSION:** full relevant pytest, Ruff, mypy, and `git diff --check` pass.

Every production-code change follows RED -> observed expected failure -> GREEN -> regression checks.

## New canary protocol

1. Keep the old run in `OPERATOR_RECOVERY_REQUIRED`; do not resolve it.
2. Create a new job ID from the same approved content scope and frozen public evidence.
3. Freeze a new execution plan with:

```text
provider_ids=("minimax-oauth",)
model_ids=("MiniMax-M3",)
maximum_retries=0
```

4. Obtain a new explicit paid-execution approval because the plan fingerprint changes.
5. Generate a fresh outline packet with a new job ID and new input hash.
6. Before each model stage, visibly confirm the active session identity is `minimax-oauth / MiniMax-M3` and that no `/model` switch occurred.
7. Execute `outline -> draft -> critic -> revision` with fresh envelopes; do not reuse old output.
8. Bind each completion with exact MiniMax attestation.
9. Finalize only after `FINAL_QA_READY` and verify the immutable artifact bundle and final `SUCCEEDED` state.

## Security and provenance invariants

- The Hermes global default is configuration evidence, not proof of a past session's actual model.
- No provider call is performed by the rail.
- No secrets, OAuth tokens, credentials, or auth files enter packets, ledger, artifacts, or logs.
- No completion is edited to repair identity or provenance.
- No old completion is attached to the new job because `job_id` and `input_hash` must be fresh.
- No automatic retry, provider fallback, model fallback, or background execution is introduced.
- A model switch requires a newly approved plan and new run; it cannot be repaired after bind.

## Scope boundaries

Included:

- plan-bound provider/model identity;
- supervised contract tests and documentation;
- one new local MiniMax M3 canary through immutable artifact verification.

Excluded and still separate owner gates:

- push;
- PR creation;
- merge;
- deployment;
- CMS/n8n/Telegram publication;
- Supabase/Drive delivery;
- provider credential changes or paid API calls outside the user's visible Hermes subscription session.

## Definition of done

- No hard-coded Terra identity controls supervised packet creation, bind authority, or artifact model usage.
- Exact MiniMax identity is frozen from the approved plan through every packet and final manifest.
- Negative provenance tests prove mismatch rejection.
- New canary reaches `SUCCEEDED` with a verified immutable artifact bundle.
- Old canary remains preserved in `OPERATOR_RECOVERY_REQUIRED`.
- No push, PR, merge, deployment, or publication occurs without its later explicit owner gate.
