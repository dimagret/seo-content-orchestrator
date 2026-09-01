# Plan-bound Supervised Model Identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make supervised provider/model identity derive from the approved immutable execution plan and prove a complete `minimax-oauth / MiniMax-M3` offline lifecycle without changing historical ledgers.

**Architecture:** Introduce a frozen runtime identity value at the rail boundary. The subscription service validates exactly one approved provider and model, passes them into packet preparation, and reuses the same approved identity for final artifact model usage. Existing packet and event JSON formats remain unchanged.

**Tech Stack:** Python 3.13, dataclasses, SQLite, pytest, Ruff, mypy, uv.

## Global Constraints

- Follow RED -> observed expected failure -> GREEN for every behavior change.
- Keep the existing canary ledger immutable and in `OPERATOR_RECOVERY_REQUIRED`.
- Add no provider SDK, network client, credential access, retry, fallback, or background execution.
- Do not commit, push, open a PR, merge, deploy, or publish.
- Use `/opt/data/cache` as pytest temp root.

---

### Task 1: Explicit runtime identity at the rail boundary

**Files:**
- Modify: `src/seo_orchestrator/supervised_rail.py`
- Test: `tests/contract/test_supervised_rail.py`

**Interfaces:**
- Produces: `SupervisedRuntimeIdentity(provider_id: str, model_id: str)`.
- Changes: `build_stage_packet(..., runtime_identity=...)` and `SupervisedRail.prepare_packet(..., runtime_identity=...)` require the explicit identity.

- [ ] Add a contract test proving a MiniMax runtime identity controls packet provider/model and input hash.
- [ ] Run the single test and observe the expected failure because runtime identity is unsupported.
- [ ] Add the frozen identity type and explicit packet parameters; remove Terra constants from packet construction.
- [ ] Update existing direct rail tests to pass one explicit MiniMax fixture identity and make completion helpers attest from the packet.
- [ ] Run the complete rail contract module and confirm green.

### Task 2: Derive identity from the approved execution plan

**Files:**
- Modify: `src/seo_orchestrator/services/supervised_subscription.py`
- Test: `tests/integration/test_supervised_subscription.py`

**Interfaces:**
- Changes: `_validate_plan(job_service, job_id) -> SupervisedRuntimeIdentity`.
- Valid plans contain exactly one provider ID and one model ID.
- `prepare_supervised_packet` passes that identity into all preflight and persistence paths.

- [ ] Add integration tests proving a `minimax-oauth / MiniMax-M3` plan creates the matching packet.
- [ ] Add parameterized denied tests for zero/multiple model IDs, zero/multiple provider IDs, and nonzero retry count before job transition.
- [ ] Run the new tests and observe expected Terra-hardcode/plan rejection failures.
- [ ] Implement minimal plan-derived identity validation and explicit packet propagation.
- [ ] Update direct integration rail setup to pass identities matching each fixture plan.
- [ ] Run preparation and rail integration tests and confirm green.

### Task 3: Plan-bound final artifact and CLI lifecycle

**Files:**
- Modify: `src/seo_orchestrator/services/supervised_subscription.py`
- Test: `tests/integration/test_supervised_subscription.py`
- Test: `tests/unit/test_runner_cli.py` only if the existing AST guard or parser contract requires adjustment.

**Interfaces:**
- Changes: `_execution_result(..., runtime_identity=...)` records the approved identity in `model_usage`.
- Finalizer revalidates and supplies the approved identity.

- [ ] Change the complete offline CLI lifecycle test to use `minimax-oauth / MiniMax-M3` and assert packet/bind values.
- [ ] Add/assert final manifest model usage is exactly `minimax-oauth / MiniMax-M3`.
- [ ] Run the lifecycle test and observe expected failure from Terra-only validation or artifact constants.
- [ ] Implement minimal plan-bound final result identity.
- [ ] Run targeted supervised rail/subscription/CLI tests.

### Task 4: Security and regression verification

**Files:**
- Review only all changed source/test/docs files.

- [ ] Run the transport/process import guard proving supervised modules add no provider/network execution path.
- [ ] Run full relevant supervised test modules.
- [ ] Run full repository pytest using `/opt/data/cache` for `TMPDIR` and `--basetemp`.
- [ ] Run `ruff check src tests`.
- [ ] Run `mypy src`.
- [ ] Run `git diff --check`.
- [ ] Perform evidence-based security review of the actual diff.
- [ ] Verify the historical canary remains `OPERATOR_RECOVERY_REQUIRED` with three accepted completions and no artifact.
- [ ] Report local changes and evidence; stop before commit.
