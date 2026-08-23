# Staged Mock Pipeline v1 Implementation Plan

> **Execution mode:** inline TDD in isolated worktree; no external calls or profile mutations.

**Goal:** Make `worker --mock` complete a deterministic, restart-safe staged execution and publish a valid immutable artifact instead of remaining `RUNNING` forever.

**Architecture:** Add a `StagedMockExecutor` that composes the existing durable `MockExecutor`. The existing executor remains the authority for immutable submission identity, approval deadline, provider/model authorization, lookup, and cancellation. A second table in the same local mock SQLite database stores the canonical terminal result, its SHA-256, and a monotonic stage cursor. Polling exposes one fixed stage at a time and atomically advances the cursor; after the final stage it rehydrates the canonical result through the existing validator and returns `SUCCEEDED`.

**Scope constraints:**

- Local/mock only; zero provider, HTTP, DNS, n8n, Google, Telegram, or Hermes profile calls.
- Preserve the public 13-tool API and the core runner/database schema.
- Do not change existing scriptable `MockExecutor` behavior or tests.
- Do not claim research occurred: mock output has `sources=()` and an explicit warning.
- Derive output only from the frozen `ExecutionSnapshot` and configured mock identities.
- Fail closed on missing, non-canonical, hash-mismatched, version-mismatched, or out-of-range durable state.
- Keep cancellation terminal and idempotent through the composed `MockExecutor`.

## Fixed stage contract

1. `freeze_context`
2. `acquire_sources`
3. `normalize_sources`
4. `analyze_competitors`
5. `research_facts`
6. `merge_evidence`
7. `build_outline`
8. `write_draft`
9. `deterministic_qa`
10. `critic_review`
11. `reader_review`
12. `revise_draft`
13. `build_metadata`
14. `freeze_artifact`

The stages model orchestration progress only. The mock performs no external acquisition and records zero deterministic stage durations.

## Task 1 — Pure deterministic result builder

**Files:**
- Create: `src/seo_orchestrator/executors/staged_mock.py`
- Create: `tests/unit/test_staged_mock_executor.py`

- [ ] RED: prove identical frozen snapshots produce identical canonical result bytes.
- [ ] RED: prove output uses the brief primary keyword, has five titles/descriptions, empty sources, explicit mock warning, exact configured model/provider identities, and all fixed stage timing keys.
- [ ] RED: prove malformed/incomplete snapshot context fails closed.
- [ ] GREEN: implement strict context extraction and `ExecutionResult` construction using existing `execution_result_bytes` validation.
- [ ] REFACTOR: keep generation pure and network-free.

Run:
`uv run pytest tests/unit/test_staged_mock_executor.py -q --basetemp=/opt/data/t21/u1`

## Task 2 — Durable stage executor

**Files:**
- Modify: `src/seo_orchestrator/executors/staged_mock.py`
- Modify: `tests/unit/test_staged_mock_executor.py`

- [ ] RED: submit returns the existing semantic idempotency key and duplicate submit is deduplicated.
- [ ] RED: each poll returns the next fixed stage, then one canonical `SUCCEEDED` result.
- [ ] RED: a new executor instance continues from the exact next stage.
- [ ] RED: cancel remains idempotent and terminal across restart.
- [ ] RED: tampered result/hash/version/cursor fails closed without returning an artifact.
- [ ] GREEN: compose `MockExecutor`; initialize `staged_mock_runs` idempotently and advance with `BEGIN IMMEDIATE`.
- [ ] REFACTOR: delegate all existing authority/capability properties and methods rather than duplicating them.

Run:
`uv run pytest tests/unit/test_staged_mock_executor.py tests/unit/test_mock_executor.py -q --basetemp=/opt/data/t21/u2`

## Task 3 — Explicit CLI wiring

**Files:**
- Modify: `src/seo_orchestrator/cli.py`
- Modify: `tests/unit/test_runner_cli.py`

- [ ] RED: `worker --mock` selects `StagedMockExecutor` with durable state.
- [ ] RED: production still rejects mock selection and no default executor is introduced.
- [ ] GREEN: replace only the explicit mock CLI factory.

Run:
`uv run pytest tests/unit/test_runner_cli.py -q --basetemp=/opt/data/t21/u3`

## Task 4 — End-to-end artifact acceptance

**Files:**
- Create: `tests/integration/test_staged_mock_pipeline.py`

- [ ] RED: queue one approved local job, restart the staged executor mid-pipeline, tick to completion, and assert `SUCCEEDED` plus immutable manifest/artifact files.
- [ ] RED: assert persisted stage sequence is monotonic and final metadata truthfully records mock/no-source behavior.
- [ ] GREEN: make only the smallest implementation changes needed for the real runner path.

Run:
`uv run pytest tests/integration/test_staged_mock_pipeline.py -q --basetemp=/opt/data/t21/i1`

## Task 5 — Verification and review

- [ ] Run targeted unit/integration suites.
- [ ] Run `uv run ruff check src tests`.
- [ ] Run `uv run mypy src integrations`.
- [ ] Run `uv lock --check` and `git diff --check`.
- [ ] Run full `uv run pytest -q` with a short `/opt/data` basetemp.
- [ ] Confirm repository diff contains no external integration/profile/deployment changes.
- [ ] Freeze exact candidate hashes and obtain two independent read-only reviews before any delivery action.

No commit, push, PR, merge, plugin installation, deployment, or external call is authorized by this plan.