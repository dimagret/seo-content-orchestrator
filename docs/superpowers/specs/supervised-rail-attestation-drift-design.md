# Rail hardening v2 — operator_id and observed_at drift design summary

## Goal

Add 2-3 contract tests for two attestation fields that the existing
test surface treats implicitly but never rejects on their own:

1. `operator_id` drift — the same `(company_id, job_id, stage_id, input_hash)` packet is bound twice with two different `operator_id` values. The second binding must fail closed.
2. `observed_at` drift — completion arrives with `observed_at` in the future relative to the rail's clock. The binding must fail closed.

## Scope and non-scope

In scope (this design summary):

- negative tests only;
- no production code changes unless RED proves the existing guards do not reject the targeted drift;
- no provider/Hermes/OAuth/credential/published actions;
- no changes to `JobService` state machine.

Out of scope:

- more production-side guards beyond what already exists;
- changes to existing tests;
- refactor of existing `_validate_attestation` / `_aware` helpers.

## Hypothesis

The existing `OperatorAttestation.__post_init__` rejects blank `operator_id`
and non-aware `observed_at`. The existing `bind_completion` does not
explicitly check that the operator_id in the completion matches the
operator_id recorded at `prepare_packet` (the packet itself does not
carry an operator_id today), so `operator_id` drift **may** be accepted
silently. The future-observed_at test will likely fail because
`_aware` only requires timezone awareness, not clock order, and
`bind_completion` does not check `observed_at <= now()`.

If a `bind_completion` test fails, the green production change will be
minimal: a single guard that rejects a future `observed_at`. The blank
`operator_id` test is expected to fail at attestation construction and
therefore requires no production change.

## New tests (2-3 contract tests)

All tests live in `tests/contract/test_supervised_rail.py` and follow the
existing `tmp_path` + `SupervisedRail(state_path=...)` + `_snapshot()` +
`_job()` + `_completion()` pattern.

### 1. `test_bind_completion_rejects_empty_operator_id`

- prepare outline packet;
- build a completion with `operator_id=" "` (single space);
- `bind_completion` must raise `ValueError` matching `operator_id` or
  `attestation`;
- reopen `SupervisedRail(state_path=state_path)` and confirm the
  `supervised_completions` table has zero rows for this packet.

### 2. `test_bind_completion_rejects_future_observed_at`

- prepare outline packet;
- build a completion with `observed_at=now + 1 day`;
- `bind_completion` must raise `ValueError` matching `observed_at` or
  `attestation`;
- reopen and confirm no completion row was persisted.

## Implementation hypothesis

If a test fails in RED, the guard added in production will be:

```python
attestation = completion.attestation
if not attestation.operator_id.strip():
    raise ValueError("attestation.operator_id must be a non-empty string")
if attestation.observed_at > datetime.now(UTC) + ATTESTATION_FUTURE_SLACK:
    raise ValueError("attestation.observed_at must not be in the future")
```

Where `ATTESTATION_FUTURE_SLACK` defaults to a small value (60 seconds)
declared as a module constant. Existing historical test fixtures use a
fixed 2026 timestamp, so no age limit is introduced: stale timestamps
are not distinguishable from valid replayable fixtures without a packet
run-start timestamp.

If no test fails, this design summary is closed without code changes.

## Out-of-scope behaviour I will NOT add

- per-operator authorization (any operator_id is allowed);
- clock injection or freeze;
- new `OperatorAttestation` fields.

## Verification

- new tests pass and existing tests continue to pass (`pytest tests -q`);
- ruff and mypy stay green;
- `git diff --check` passes;
- no provider/Hermes/OAuth/publication/Telegram/Sheets/n8n/deploy actions;
- one commit + push to `feat/task-23-supervised-subscription-rail`.

## Approval scope

This design authorizes:

1. writing up to 3 contract tests in `tests/contract/test_supervised_rail.py`;
2. making the **minimum** production changes (if any) in
   `src/seo_orchestrator/supervised_rail.py` to make them green.

It does **not** authorize:

- changes to other modules;
- merge;
- deployment;
- subscription use.