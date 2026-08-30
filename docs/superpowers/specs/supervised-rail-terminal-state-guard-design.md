# Rail hardening — terminal-state guard design summary

## Goal

Cover the remaining local-only negative paths around terminal states of the supervised subscription rail: `FINAL_QA_READY` and `ARTIFACT_FROZEN`. These are reached only through four operator-attested stage bindings followed by `SupervisedSubscriptionFinalizer.finalize`. Once terminal, the rail must not accept any state-changing calls that contradict the local contract.

## Scope and non-scope

In scope (this design summary):

- negative tests only;
- no production code changes;
- no provider/Hermes/OAuth/credential/published actions;
- no changes to `JobService` state machine;
- the existing supervised rail API (`prepare_packet`, `bind_completion`, `mark_operator_recovery_required`, `request_cancel`, `resolve_recovery_for_exact_binding`, `record_artifact`).

Out of scope:

- additional production-side guards beyond what already exists;
- changes to existing tests;
- refactor of existing `_final_QA_*` paths.

## Coverage matrix

| Terminal state | Already blocked | New test to add |
| --- | --- | --- |
| `FINAL_QA_READY` — `bind_completion` | `test_completion_advances_only_through_fixed_four_stage_order` covers "next bind returns FINAL_QA_READY status, no new packet" | Confirm `mark_operator_recovery_required` and `request_cancel` after FINAL_QA_READY are rejected |
| `FINAL_QA_READY` — `prepare_packet` | Implicit: rail no longer has outstanding packet | Confirm |
| `FINAL_QA_READY` — `mark_operator_recovery_required` | not covered | new test |
| `FINAL_QA_READY` — `request_cancel` | not covered | new test |
| `ARTIFACT_FROZEN` — `bind_completion` | Implicit: rail no longer has outstanding packet | Confirm |
| `ARTIFACT_FROZEN` — `prepare_packet` | Implicit: rail no longer has outstanding packet | Confirm |
| `ARTIFACT_FROZEN` — `mark_operator_recovery_required` | not covered | new test |
| `ARTIFACT_FROZEN` — `request_cancel` | not covered | new test |
| `ARTIFACT_FROZEN` — second `record_artifact` with same identity | `test_artifact_pointer_is_write_once_in_sqlite` covers pointer write-once | Confirm path is also blocked at the rail surface |

## New tests (4 contract tests)

All tests live in `tests/contract/test_supervised_rail.py` and follow the existing `tmp_path` + `SupervisedRail(state_path=...)` + `_snapshot()` + `_job()` pattern.

### 1. `test_mark_operator_recovery_required_is_rejected_after_final_qa_ready`

- prepare outline packet, bind outline, critic, draft, then revision completion so that the rail reaches `FINAL_QA_READY`;
- assert `status is FINAL_QA_READY`;
- call `rail.mark_operator_recovery_required(...)`;
- assert it raises `ValueError` with message containing `FINAL_QA_READY`;
- reopen `SupervisedRail(state_path=state_path)` and assert the same `mark_operator_recovery_required` still raises `ValueError`;
- assert the supervised_events ledger contains no `OPERATOR_RECOVERY_REQUIRED` row.

### 2. `test_request_cancel_is_rejected_after_final_qa_ready`

- same setup as #1;
- call `rail.request_cancel(operator_id=...)`;
- assert it raises `ValueError` with message containing `FINAL_QA_READY`;
- reopen and assert the same;
- assert the supervised_events ledger contains no `CANCEL_REQUESTED` row.

### 3. `test_mark_operator_recovery_required_is_rejected_after_artifact_frozen`

- run the full happy path to `ARTIFACT_FROZEN` (this requires an authorized `JobService` and `ArtifactStore` fixture; reuse the `tests/integration/test_supervised_subscription.py` plumbing or extract a minimal helper);
- call `rail.mark_operator_recovery_required(...)`;
- assert it raises `ValueError` with message containing `ARTIFACT_FROZEN`;
- reopen and assert the same;
- assert no `OPERATOR_RECOVERY_REQUIRED` row in supervised_events.

### 4. `test_request_cancel_is_rejected_after_artifact_frozen`

- same setup as #3;
- call `rail.request_cancel(operator_id=...)`;
- assert it raises `ValueError` with message containing `ARTIFACT_FROZEN`;
- reopen and assert the same;
- assert no `CANCEL_REQUESTED` row in supervised_events.

## Implementation hypothesis

I expect tests #1 and #2 to pass already (no production change required). Tests #3 and #4 may need a single-line production guard in `mark_operator_recovery_required` and `request_cancel` that rejects when current status is `ARTIFACT_FROZEN`. If so, the guard will:

- check current status under `BEGIN IMMEDIATE`;
- raise `ValueError("supervised run is ARTIFACT_FROZEN")`;
- never append any event;
- never modify `supervised_jobs.status`.

This matches the existing pattern for `CANCEL_REQUESTED`, `FINAL_QA_READY`, and `OPERATOR_RECOVERY_REQUIRED`. I will only add guards if TDD proves the existing production rejects the call (RED), and only the minimum code to make it GREEN.

## Out-of-scope behavior I will NOT add

- "soft" cancel that only logs;
- "operator override" code path that ignores terminal state;
- new `SupervisedStatus` values;
- new event types.

## Verification

- new tests pass and existing tests continue to pass (`pytest tests/contract tests/integration tests/unit -q`);
- ruff and mypy stay green;
- `git diff --check` passes;
- no provider/Hermes/OAuth/publication/Telegram/Sheets/n8n/deploy actions;
- one commit + push to `feat/task-23-supervised-subscription-rail`.

## Approval scope

This design authorizes:

1. writing 4 contract tests in `tests/contract/test_supervised_rail.py`;
2. making the **minimum** production changes (if any) in `src/seo_orchestrator/supervised_rail.py` to make them green.

It does **not** authorize:

- changes to other modules;
- merge;
- deployment;
- subscription use.