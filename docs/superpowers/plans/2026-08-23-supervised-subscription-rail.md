# Supervised Subscription Rail Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local-only, operator-supervised four-stage content rail that prepares canonical packets for a visible Hermes subscription session and accepts only operator-attested, packet-bound completions.

**Architecture:** Add a dedicated supervised rail instead of extending `Executor` or `Runner`: those interfaces require provider-level durable idempotency and terminal cancellation that the consumer Codex transport cannot prove. A private SQLite ledger owns packets, observed completions, immutable bindings, and recovery state; an owner-only integrity key authenticates its per-job predecessor chain and immutable event head. Its public CLI only creates packets, records an operator-attested result, and reports local status. The rail never calls a provider or Hermes proxy.

**Tech Stack:** Python 3.13, stdlib `sqlite3`, existing canonical JSON/hash primitives, existing `ExecutionSnapshot`/`SeoJob`/`ExecutionResult` contracts, pytest.

## Global Constraints

- No direct provider HTTP, Hermes proxy, OAuth/auth-file access, API key, listener, background worker, or model invocation code.
- Fixed provider/model metadata is `openai-codex` / `gpt-5.6-terra`; it is expected packet metadata and operator-attested session configuration, not model-generated proof.
- The frozen execution plan must set `maximum_retries=0`; exactly four stage IDs are allowed: `outline`, `draft`, `critic`, `revision`; no automatic retry and no automatic stage advance.
- Use the job's existing approval binding, snapshot hash, and approved plan fingerprint; reject mismatches before any ledger write that creates a completion.
- The final output is local immutable artifact material only; no Telegram, Sheets, n8n, export, publication, or deployment path.
- Use `TMPDIR=/opt/data/cache` and an explicit short `--basetemp` under `/opt/data/cache` for pytest.
- Do not commit, push, create a PR, alter `/opt/hermes`, or execute a subscription model stage.

---

## File structure

- Create: `src/seo_orchestrator/supervised_rail.py` — canonical packet/completion value objects and private SQLite ledger.
- Create: `src/seo_orchestrator/services/supervised_subscription.py` — coordinates a terminal locally accepted revision with the existing `JobService` and `ArtifactStore`; it has no provider transport.
- Modify: `src/seo_orchestrator/cli.py` — local-only `supervised-packet`, `supervised-bind`, and `supervised-status` commands; no provider wiring.
- Modify: `src/seo_orchestrator/__init__.py` — export only the stable supervised-rail public types if this package already exports domain-facing helpers.
- Create: `tests/contract/test_supervised_rail.py` — unit/contract tests for packet identity, attestation binding, recovery, cancellation, and invalid data.
- Create: `tests/integration/test_supervised_subscription_rail.py` — end-to-end local fixture proving the four packet sequence and immutable final-result bundle without a model call.
- Create: `docs/superpowers/specs/2026-08-23-supervised-subscription-rail-design.md` — copy the approved design from `/opt/data/hermes-openai-codex-broker/docs/supervised-subscription-rail-design.md` into the implementation repository unchanged except for a provenance link.

## Shared interfaces

```python
SUPERVISED_PIPELINE_VERSION = "supervised-subscription-v1"
STAGE_IDS = ("outline", "draft", "critic", "revision")

@dataclass(frozen=True, slots=True)
class StagePacket:
    job_id: str
    company_id: str
    stage_id: str
    sequence: int
    approval_record_id: str
    approved_plan_fingerprint: str
    snapshot_hash: str
    provider_id: str
    model_id: str
    input_hash: str
    prompt: str

@dataclass(frozen=True, slots=True)
class OperatorAttestation:
    session_ref: str
    observed_at: datetime
    operator_id: str

@dataclass(frozen=True, slots=True)
class ObservedCompletion:
    job_id: str
    company_id: str
    stage_id: str
    input_hash: str
    payload: JsonValue
    attestation: OperatorAttestation
```

```python
class SupervisedRail:
    def prepare_packet(self, job: SeoJob, snapshot: ExecutionSnapshot) -> StagePacket: ...
    def bind_completion(self, completion: ObservedCompletion) -> StagePacket: ...
    def status(self, *, company_id: str, job_id: str) -> SupervisedStatus: ...
    def request_cancel(self, *, company_id: str, job_id: str, operator_id: str) -> SupervisedStatus: ...
```

`prepare_packet` moves only from `PACKET_READY` to `AWAITING_OPERATOR_EXECUTION`. `bind_completion` accepts exactly the currently outstanding immutable packet and returns the next packet or final status; it never constructs a replacement attempt. `request_cancel` blocks future packets and returns `CANCEL_REQUESTED`, never claims upstream cancellation.

### Task 1: Canonical packet and observed-completion contracts

**Files:**
- Create: `src/seo_orchestrator/supervised_rail.py`
- Test: `tests/contract/test_supervised_rail.py`

**Interfaces:**
- Consumes: `SeoJob`, `ExecutionSnapshot`, `JsonValue`, `canonical_json`, and `sha256_fingerprint`.
- Produces: `StagePacket`, `OperatorAttestation`, `ObservedCompletion`, `STAGE_IDS`, and canonical packet/completion byte helpers.

- [ ] **Step 1: Write the failing packet-determinism test**

```python
def test_packet_is_identical_for_the_same_frozen_job_and_stage() -> None:
    packet_one = build_stage_packet(job, snapshot, stage_id="outline", sequence=0)
    packet_two = build_stage_packet(job, snapshot, stage_id="outline", sequence=0)

    assert packet_one == packet_two
    assert packet_one.input_hash == sha256_fingerprint(packet_identity_mapping(packet_one))
    assert packet_one.provider_id == "openai-codex"
    assert packet_one.model_id == "gpt-5.6-terra"
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-red-1 tests/contract/test_supervised_rail.py::test_packet_is_identical_for_the_same_frozen_job_and_stage -q`

Expected: FAIL because `seo_orchestrator.supervised_rail` does not exist.

- [ ] **Step 3: Implement only canonical value-object validation and packet construction**

```python
def build_stage_packet(job: SeoJob, snapshot: ExecutionSnapshot, *, stage_id: str, sequence: int) -> StagePacket:
    _validate_frozen_job_binding(job, snapshot)
    body = _packet_body(job, snapshot, stage_id=stage_id, sequence=sequence)
    return StagePacket(**body, input_hash=sha256_fingerprint(body))
```

`packet_identity_mapping()` must contain every displayed packet field except the self-referential `input_hash`; hash that mapping to produce `input_hash`. Validate exact stage membership, non-negative sequence, job/snapshot company/hash equality, non-empty approved plan/approval IDs, and lower-case SHA-256 values. Do not open SQLite or make a model call in this task.

- [ ] **Step 4: Run focused contract tests and verify GREEN**

Run: `TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-green-1 tests/contract/test_supervised_rail.py -q`

Expected: packet determinism plus malformed stage, mismatched snapshot, missing approval, changed sequence, and malformed attestation tests pass.

- [ ] **Step 5: Keep the worktree uncommitted**

Do not run `git commit`, `git push`, or any command that contacts a remote.

### Task 2: Private immutable local ledger and state machine

**Files:**
- Modify: `src/seo_orchestrator/supervised_rail.py`
- Modify: `tests/contract/test_supervised_rail.py`

**Interfaces:**
- Consumes: Task 1 packet/completion objects.
- Produces: `SupervisedRail`, `SupervisedStatus`, and statuses `PACKET_READY`, `AWAITING_OPERATOR_EXECUTION`, `COMPLETION_SUBMITTED`, `ACCEPTED`, `REJECTED`, `OPERATOR_RECOVERY_REQUIRED`, and `CANCEL_REQUESTED`.

- [ ] **Step 1: Write the failing one-packet/one-completion test**

```python
def test_rail_rejects_second_or_changed_completion_for_the_same_packet(tmp_path: Path) -> None:
    rail = SupervisedRail(state_path=tmp_path / "rail.db")
    packet = rail.prepare_packet(job, snapshot)
    accepted = rail.bind_completion(valid_completion(packet, session_ref="session-1"))

    assert accepted.stage_id == "draft"
    with pytest.raises(ValueError, match="already bound"):
        rail.bind_completion(valid_completion(packet, session_ref="session-1"))
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-red-2 tests/contract/test_supervised_rail.py::test_rail_rejects_second_or_changed_completion_for_the_same_packet -q`

Expected: FAIL because `SupervisedRail` is not implemented.

- [ ] **Step 3: Implement the isolated SQLite ledger with immutable records**

Create tables for `supervised_jobs`, `supervised_packets`, `supervised_completions`, and `supervised_events`. Use canonical JSON bytes plus SHA-256 for packet and completion commitments. Add SQLite triggers rejecting updates/deletes of packet identity and accepted completion payload/attestation. Enforce one outstanding packet per `(company_id, job_id)` and unique `(company_id, job_id, stage_id, sequence)`.

`bind_completion` must check company/job/stage/input hash against the outstanding packet before inserting its immutable completion. It returns `OPERATOR_RECOVERY_REQUIRED` for an unknown execution state; it never creates another stage or calls a transport.

- [ ] **Step 4: Run focused contract tests and verify GREEN**

Run: `TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-green-2 tests/contract/test_supervised_rail.py -q`

Expected: tests pass for restart reload, duplicate completion, changed payload, cross-company binding, unknown packet, and trigger tamper rejection.

- [ ] **Step 5: Keep the worktree uncommitted**

Do not run `git commit`, `git push`, or any command that contacts a remote.

### Task 3: Four-stage progression, operator recovery, and cancellation semantics

**Files:**
- Modify: `src/seo_orchestrator/supervised_rail.py`
- Modify: `tests/contract/test_supervised_rail.py`

**Interfaces:**
- Consumes: Task 2 ledger and canonical completions.
- Produces: next stage packet in the fixed order and terminal local states.

- [ ] **Step 1: Write the failing progression and recovery tests**

```python
def test_completion_advances_only_through_the_fixed_four_stage_order(tmp_path: Path) -> None:
    rail = SupervisedRail(state_path=tmp_path / "rail.db")
    packet = rail.prepare_packet(job, snapshot)
    for expected in ("draft", "critic", "revision"):
        packet = rail.bind_completion(valid_completion(packet, session_ref="session-1"))
        assert packet.stage_id == expected

    status = rail.bind_completion(valid_completion(packet, session_ref="session-1"))
    assert status.state == "FINAL_QA_READY"


def test_unknown_operator_execution_never_retries(tmp_path: Path) -> None:
    rail = SupervisedRail(state_path=tmp_path / "rail.db")
    rail.prepare_packet(job, snapshot)
    status = rail.mark_operator_recovery_required(company_id=job.company_id, job_id=job.job_id)

    assert status.state == "OPERATOR_RECOVERY_REQUIRED"
    with pytest.raises(ValueError, match="operator recovery"):
        rail.prepare_packet(job, snapshot)
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-red-3 tests/contract/test_supervised_rail.py -k 'fixed_four_stage_order or unknown_operator_execution_never_retries' -q`

Expected: FAIL because fixed-order progression and recovery methods are missing.

- [ ] **Step 3: Implement fixed sequencing and fail-closed recovery/cancel behavior**

Advance only from `outline` to `draft`, `draft` to `critic`, `critic` to `revision`, and `revision` to `FINAL_QA_READY`. `mark_operator_recovery_required` blocks both bind and prepare until a separately recorded operator resolution; do not implement any automatic resolution. `request_cancel` sets `CANCEL_REQUESTED`, records the operator ID/time, and rejects prepare/bind; it must not report `CANCELED` as an upstream outcome.

- [ ] **Step 4: Run focused contract tests and verify GREEN**

Run: `TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-green-3 tests/contract/test_supervised_rail.py -q`

Expected: progression, restart, recovery, cancellation, and no-auto-retry tests pass.

- [ ] **Step 5: Keep the worktree uncommitted**

Do not run `git commit`, `git push`, or any command that contacts a remote.

### Task 4: Deterministic final-result acceptance and immutable local artifact

**Files:**
- Modify: `src/seo_orchestrator/supervised_rail.py`
- Create: `src/seo_orchestrator/services/supervised_subscription.py`
- Create: `tests/integration/test_supervised_subscription_rail.py`

**Interfaces:**
- Consumes: accepted `revision` completion, `ExecutionResult`, `JobService.transition`, `JobService.bind_artifact_manifest`, and `ArtifactStore`.
- Produces: a validated `ExecutionResult`, a `RUNNING → SUCCEEDED` audited local job transition, and an `ArtifactManifest` written only after `FINAL_QA_READY`.

- [ ] **Step 1: Write the failing local-only end-to-end test**

```python
def test_four_operator_attested_stages_create_one_local_immutable_artifact(tmp_path: Path) -> None:
    rail = SupervisedRail(state_path=tmp_path / "rail.db")
    packet = rail.prepare_packet(flow.job, flow.snapshot)
    for stage_payload in valid_stage_payloads(flow):
        outcome = rail.bind_completion(valid_completion(packet, payload=stage_payload))
        if isinstance(outcome, StagePacket):
            packet = outcome

    manifest = SupervisedSubscriptionFinalizer(rail, job_service, artifact_store).finalize(flow.job.job_id)

    assert manifest.company_id == flow.job.company_id
    assert not any("provider" in name for name in artifact_file_names(manifest))
    assert rail.status(company_id=flow.job.company_id, job_id=flow.job.job_id).state == "ARTIFACT_FROZEN"
```

- [ ] **Step 2: Run the focused integration test and verify RED**

Run: `TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-red-4 tests/integration/test_supervised_subscription_rail.py::test_four_operator_attested_stages_create_one_local_immutable_artifact -q`

Expected: FAIL because final-result validation and artifact writing are missing.

- [ ] **Step 3: Implement bounded final payload validation and artifact writing**

Define an exact revision payload mapping with `content_markdown`, five `titles`, five `descriptions`, `sources`, and `warnings`. Reject hidden-reasoning keys, unexpected keys, secret-like values, oversized strings, malformed source objects, and citations absent from the frozen evidence set. Derive `keyword_qa`, `text_metrics`, zero token usage, fixed four-stage timings, and `prompt_versions={"pipeline": "supervised-subscription-v1"}` locally.

`SupervisedSubscriptionFinalizer.finalize(job_id)` must first prove the rail is `FINAL_QA_READY`, then call the existing scoped `JobService.transition(job_id, JobState.RUNNING, JobState.SUCCEEDED, "supervised subscription final QA accepted", current_stage="revision")`. It passes the returned succeeded `SeoJob` to `ArtifactStore.write_bundle`, calls `JobService.bind_artifact_manifest(job_id)`, and only then stores the verified manifest path/hash in the rail ledger. A later finalize call must verify the same existing manifest/result or fail closed; it must never write a second result.

- [ ] **Step 4: Run focused integration tests and verify GREEN**

Run: `TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-green-4 tests/integration/test_supervised_subscription_rail.py -q`

Expected: the four-stage fixture writes one artifact; changed completion, invalid citation, post-write mutation, and second-write tests pass.

- [ ] **Step 5: Keep the worktree uncommitted**

Do not run `git commit`, `git push`, or any command that contacts a remote.

### Task 5: Local CLI and regression verification

**Files:**
- Modify: `src/seo_orchestrator/cli.py`
- Create: `tests/unit/test_supervised_rail_cli.py`
- Modify: `tests/integration/test_supervised_subscription_rail.py`
- Create: `docs/superpowers/specs/2026-08-23-supervised-subscription-rail-design.md`

**Interfaces:**
- Consumes: `SupervisedRail` public methods and `SupervisedSubscriptionFinalizer` from Tasks 1–4.
- Produces: offline CLI commands that serialize packets/status as canonical JSON and bind a local completion file without executing a provider call.

- [ ] **Step 1: Write failing CLI contract tests**

```python
def test_supervised_packet_command_emits_one_canonical_packet_without_provider_call(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli.Settings, "from_env", lambda _env: settings)
    monkeypatch.setattr(cli, "supervised_packet", lambda *_args, **_kwargs: packet)

    cli.main([
        "supervised-packet",
        "--job-id", "job-one",
        "--company-id", "acme",
        "--session-ref", "approved-visible-session",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert payload["stage_id"] == "outline"
    assert payload["provider_id"] == "openai-codex"
    assert payload["model_id"] == "gpt-5.6-terra"
```

- [ ] **Step 2: Run the focused CLI test and verify RED**

Run: `TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-red-5 tests/contract/test_supervised_rail.py::test_supervised_packet_command_emits_one_canonical_packet_without_provider_call -q`

Expected: FAIL because the command does not exist.

- [ ] **Step 3: Add minimal offline commands and copy approved design**

Add `supervised-packet`, `supervised-bind`, and `supervised-status` with explicit `--company-id` and `--job-id`. Derive the one supervised ledger path from the verified authoritative main database; expose no caller-selected state path. `supervised-packet` requires the designated visible `--session-ref`. `supervised-bind` reads a bounded canonical completion envelope containing `company_id`, `job_id`, `stage_id`, `input_hash`, and `payload`, and requires explicit `--operator-id`, `--session-ref`, `--provider-id`, and `--model-id`; it never reads Hermes session storage or credentials. Copy the approved design into this repository and link it to [`../specs/2026-08-23-codex-lifecycle-evidence.md`](../specs/2026-08-23-codex-lifecycle-evidence.md).

- [ ] **Step 4: Run focused tests, full suite, static checks, and diff validation**

Run:

```bash
TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-full tests/contract/test_supervised_rail.py tests/integration/test_supervised_subscription.py tests/unit/test_runner_cli.py -q
TMPDIR=/opt/data/cache uv run pytest --basetemp=/opt/data/cache/t23-all tests -q
git diff --check
uv run ruff check src tests
uv run mypy src
```

Expected: every command exits `0`; no provider request, Hermes proxy process, credential read, network acquisition, or artifact delivery occurs during verification.

- [ ] **Step 5: Keep the worktree uncommitted and report evidence**

Do not run `git commit`, `git push`, PR, merge, provider, proxy, publication, Telegram, Sheets, or n8n commands. Report exact command output and the remaining G4 gate.
