# Telegram SEO Orchestrator Stage B MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a durable multi-company SEO orchestration MVP controlled through Gul’dan in Telegram, using one universal n8n workflow copy as the temporary execution engine without mutating n8n nodes per company.

**Architecture:** A profile-local Hermes plugin exposes narrow agent tools and talks over a Unix domain socket to an isolated Python worker. The worker owns versioned company cards, immutable execution snapshots, approvals, jobs, artifacts, and notifications in SQLite/WAL; after a separate approval it submits signed jobs to one parameterized n8n workflow copy. Company changes create database versions and snapshots only—never n8n node mutations.

**Tech Stack:** Python 3.13, uv, FastAPI, Uvicorn, Pydantic v2, stdlib SQLite/HMAC/hashlib/socket, httpx, pytest, pytest-asyncio, Ruff, mypy, rootless Docker, user systemd, Hermes profile-local plugin API.

## Global Constraints

- Work in `/opt/data/seo-content-orchestrator`; do not modify `/opt/hermes` core.
- Use an isolated git worktree when implementation begins.
- Follow strict RED → GREEN → REFACTOR TDD for every task.
- The original n8n workflow `0ORDwUdQfn3dkEut` remains unchanged.
- Creating, selecting, editing, or archiving a company must cause zero n8n node mutations.
- One universal Stage B workflow copy serves all companies through an immutable `ExecutionSnapshot`.
- Company cards live in the worker database, never in pinned n8n data, workflow static data, prompts, or Google Sheets.
- No paid provider calls, n8n writes, Google Sheets writes, Hermes plugin enablement, systemd installation, or live Telegram delivery without explicit approval.
- Current n8n OAuth remains read-only until a separately approved integration task.
- Production transport is Unix socket only; do not publish a TCP port.
- Gateway-visible socket path: `/opt/data/seo-runtime/worker.sock`.
- Worker-visible socket path: `/run/seo-orchestrator/worker.sock`.
- Production worker identity: UID/GID `10000:10000`; socket mode `0660`.
- Worker persistent state is isolated from the Hermes profile data volume.
- Do not expose credentials, raw provider responses, chain-of-thought, or hidden model reasoning.
- Paid execution, Google Sheets export, and publication are separate approvals.
- An approval is invalid after any snapshot or plan fingerprint change.
- Exact LLM text equality is not a parity requirement.
- SQLite is the MVP source of truth; repository interfaces must permit a later PostgreSQL adapter without domain changes.
- One active job per Telegram user in MVP; cross-company sequential jobs must use the same universal workflow.

## Frozen Shared Contracts

These names and fields are authoritative across all tasks. Do not rename them in one subsystem without changing every consumer and contract test in the same commit.

```python
JsonScalar = None | bool | int | str
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]

class JobState(StrEnum):
    DRAFT = "DRAFT"
    VALIDATED = "VALIDATED"
    PLANNED = "PLANNED"
    AWAITING_PAID_APPROVAL = "AWAITING_PAID_APPROVAL"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_FINAL = "FAILED_FINAL"
    CANCELED = "CANCELED"
    SUCCEEDED = "SUCCEEDED"
    AWAITING_EXPORT_APPROVAL = "AWAITING_EXPORT_APPROVAL"
    EXPORTED = "EXPORTED"

class ExternalStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_FINAL = "FAILED_FINAL"
    CANCELED = "CANCELED"
```

Domain version keys:

```text
CompanyProfile: company_id, company_profile_id, company_profile_version
BusinessDirection: company_id, company_profile_version, direction_id, direction_version
AudienceSegment: company_id, direction_id, direction_version, audience_segment_id, audience_version
SeoBrief: brief_id, company_id, company_profile_version, direction_id, direction_version,
          audience_segment_id, audience_version
ExecutionSnapshot: snapshot_id, brief_id, company_id, company_profile_version,
                   direction_id, direction_version, audience_segment_id, audience_version,
                   prompt_set_version, compiled_context, snapshot_hash, created_at
```

The complete content fields are the exact fields listed in design spec Section 6; the version keys above are mandatory foreign-key scope and may not be inferred from nested content.

Shared dataclasses/models:

```python
@dataclass(frozen=True)
class ExecutionPlan:
    pipeline_version: str
    executor_name: str
    model_ids: tuple[str, ...]
    provider_ids: tuple[str, ...]
    maximum_retries: int
    cost_currency: str | None
    cost_min_decimal: str | None
    cost_max_decimal: str | None
    unknown_cost_reasons: tuple[str, ...]
    result_destination: str

@dataclass(frozen=True)
class PlannedJob:
    job_id: str
    snapshot_id: str
    snapshot_hash: str
    plan: ExecutionPlan
    plan_fingerprint: str
    state: JobState

@dataclass(frozen=True)
class ExternalRun:
    external_run_id: str
    idempotency_key: str
    accepted_at: datetime

@dataclass(frozen=True)
class ExecutionStatus:
    external_run_id: str
    status: ExternalStatus
    stage_id: str | None
    retry_after_seconds: int | None
    error_code: str | None
    error_summary: str | None
    result: "ExecutionResult | None"

@dataclass(frozen=True)
class ExecutionResult:
    content_markdown: str
    titles: tuple[str, str, str, str, str]
    descriptions: tuple[str, str, str, str, str]
    keyword_qa: JsonValue
    text_metrics: JsonValue
    sources: tuple[JsonValue, ...]
    warnings: tuple[str, ...]
    model_usage: JsonValue
    stage_timings: JsonValue
    prompt_versions: JsonValue

@dataclass(frozen=True)
class ArtifactManifest:
    job_id: str
    company_id: str
    snapshot_hash: str
    artifact_hashes: dict[str, str]
    created_at: datetime

@dataclass(frozen=True)
class NotificationEvent:
    event_id: str
    job_id: str
    event_type: str
    company_display_name: str
    direction_display_name: str
    stage_id: str | None
    completed_stage_count: int
    attempt: int
    elapsed_seconds: int
    artifact_ready: bool

@dataclass(frozen=True)
class DeliveryReceipt:
    event_id: str
    destination: str
    delivered_at: datetime

@dataclass(frozen=True)
class ExportPlan:
    job_id: str
    artifact_hash: str
    spreadsheet_id: str
    sheet_name: str
    row_selector: str
    column_map: dict[str, str]
    plan_fingerprint: str

@dataclass(frozen=True)
class ExportReceipt:
    job_id: str
    export_key: str
    destination_reference: str
    exported_at: datetime
```

Security interfaces:

```python
class Resolver(Protocol):
    def resolve(self, hostname: str, port: int) -> tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, ...]: ...

@dataclass(frozen=True)
class NormalizedUrl:
    value: str
    hostname: str
    port: int
    resolved_addresses: tuple[str, ...]
```

## External Approval Gates

| Gate | Required before | Not required for |
|---|---|---|
| G1 — Local implementation | Creating source, tests, mocks, fixtures, local DBs | Reading this plan |
| G2 — Hermes test-profile install | Copying/enabling plugin in an isolated test profile | Testing plugin modules directly |
| G3 — n8n write access | Creating workflow copy, wrapper, webhook, or changing OAuth scopes | Transforming local exported JSON |
| G4 — Paid canary | Firecrawl, OpenRouter, Perplexity, Gemini, or other paid calls | Mock executor tests |
| G5 — Live Telegram delivery | Sending worker progress to the live chat | Notification sink unit tests |
| G6 — Google Sheets write | Exporting any artifact | Generating local export payload |
| G7 — Deployment | Creating Docker volumes, installing systemd unit, enabling service | Building and scanning image locally |

---

## File Map

```text
seo-content-orchestrator/
├── pyproject.toml
├── uv.lock
├── README.md
├── .gitignore
├── CONTEXT.md
├── src/seo_orchestrator/
│   ├── __init__.py
│   ├── settings.py                 # environment and filesystem policy
│   ├── canonical.py                # canonical JSON and SHA-256 fingerprints
│   ├── errors.py                   # stable public error codes
│   ├── domain/
│   │   ├── models.py               # company, direction, audience, brief, snapshot
│   │   ├── approvals.py            # approval model and invalidation rules
│   │   └── jobs.py                 # job states and transition table
│   ├── db/
│   │   ├── connection.py           # SQLite connection and transaction helpers
│   │   ├── migrations.py           # ordered schema migrations
│   │   └── repositories.py         # scoped persistence operations
│   ├── services/
│   │   ├── company_cards.py        # versioned company-card CRUD
│   │   ├── briefs.py               # resumable brief drafts and validation
│   │   ├── snapshots.py            # immutable manual compilation
│   │   ├── approvals.py            # plan approvals
│   │   ├── jobs.py                 # job commands and transition service
│   │   ├── artifacts.py            # immutable artifact bundle
│   │   └── notifications.py        # fixed status events
│   ├── security/
│   │   ├── url_policy.py           # SSRF and redirect policy
│   │   └── signatures.py           # HMAC request/callback signing
│   ├── executors/
│   │   ├── base.py                 # Executor protocol
│   │   ├── mock.py                 # deterministic test executor
│   │   └── n8n.py                  # signed n8n wrapper client
│   ├── runner.py                   # durable queue, retries, cancel, resume
│   ├── api/
│   │   ├── app.py                  # FastAPI factory
│   │   ├── auth.py                 # local API token verification
│   │   ├── company_routes.py
│   │   ├── brief_routes.py
│   │   ├── job_routes.py
│   │   ├── callback_routes.py
│   │   └── artifact_routes.py
│   └── cli.py                      # migrate, serve, worker, doctor
├── integrations/
│   ├── hermes/seo_orchestrator/
│   │   ├── plugin.yaml
│   │   ├── __init__.py
│   │   ├── client.py               # stdlib HTTP over Unix socket
│   │   ├── schemas.py              # Hermes JSON tool schemas
│   │   └── tools.py                # handlers return JSON strings
│   └── n8n/
│       ├── source-workflow.json     # sanitized local source fixture
│       ├── transform.py             # deterministic one-time parameterizer
│       ├── validate.py              # workflow invariants
│       ├── universal-contract.json  # JSON Schema for ExecutionSnapshot request
│       └── expected-node-map.json   # names/types/connection invariants
├── ops/
│   ├── Containerfile
│   ├── entrypoint.sh
│   ├── systemd/hermes-seo-orchestrator.service
│   └── scripts/preflight.sh
├── tests/
│   ├── conftest.py
│   ├── unit/
│   ├── integration/
│   ├── contract/
│   ├── security/
│   └── e2e/
├── fixtures/
│   ├── companies/avtomalyar.json
│   ├── companies/sweet-world.json
│   ├── executions/success-result.json
│   └── benchmark/manifest.json
└── docs/superpowers/
    ├── specs/2026-08-03-telegram-seo-orchestrator-design.md
    └── plans/2026-08-03-telegram-seo-orchestrator-stage-b-mvp.md
```

---

### Task 1: Repository Scaffold and Quality Gates

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `README.md`
- Create: `integrations/__init__.py`
- Create: `src/seo_orchestrator/__init__.py`
- Create: `src/seo_orchestrator/settings.py`
- Create: `tests/unit/test_settings.py`

**Interfaces:**
- Produces: `Settings.from_env(env: Mapping[str, str]) -> Settings`
- Produces: repeatable commands `uv sync --frozen`, `uv run pytest`, `uv run ruff check .`, `uv run mypy src integrations`

- [ ] **Step 1: Initialize git and create an isolated implementation worktree**

Run from `/opt/data/seo-content-orchestrator`:

```bash
git init
git add CONTEXT.md docs
git commit -m "docs: define SEO orchestrator architecture"
```

Then use the `using-git-worktrees` skill to create the implementation worktree. Expected: clean worktree on a dedicated branch.

- [ ] **Step 2: Write failing settings tests**

```python
from pathlib import Path
import pytest
from seo_orchestrator.settings import Settings


def test_settings_require_absolute_paths(tmp_path: Path):
    with pytest.raises(ValueError, match="absolute"):
        Settings.from_env({"SEO_DB_PATH": "relative.db", "SEO_ARTIFACT_ROOT": str(tmp_path)})


def test_production_rejects_tcp_transport(tmp_path: Path):
    with pytest.raises(ValueError, match="Unix socket"):
        Settings.from_env({
            "SEO_ENV": "production",
            "SEO_DB_PATH": str(tmp_path / "seo.db"),
            "SEO_ARTIFACT_ROOT": str(tmp_path / "artifacts"),
            "SEO_LISTEN": "127.0.0.1:8787",
        })
```

- [ ] **Step 3: Verify RED**

Run:

```bash
uv run pytest tests/unit/test_settings.py -v
```

Expected: collection fails because `seo_orchestrator.settings` does not exist.

- [ ] **Step 4: Create project metadata and minimal settings implementation**

Use this dependency floor in `pyproject.toml`:

```toml
[project]
name = "seo-content-orchestrator"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
  "fastapi>=0.116,<1",
  "httpx>=0.28,<1",
  "pydantic>=2.11,<3",
  "uvicorn>=0.35,<1",
]

[dependency-groups]
dev = [
  "mypy>=1.17,<2",
  "pytest>=8.4,<9",
  "pytest-asyncio>=1.1,<2",
  "ruff>=0.12,<1",
]

[tool.pytest.ini_options]
pythonpath = ["src", "."]
testpaths = ["tests"]

[tool.ruff]
target-version = "py313"
line-length = 100

[tool.mypy]
python_version = "3.13"
strict = true
```

Implement `Settings` as a frozen dataclass. Defaults:

```python
@dataclass(frozen=True)
class Settings:
    environment: str
    db_path: Path
    artifact_root: Path
    listen: str
    worker_socket_mode: int = 0o660
    max_active_jobs_per_user: int = 1

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "Settings": ...
```

Production must accept `unix:/run/seo-orchestrator/worker.sock` and reject TCP.

SEO_WORKER_SOCKET_MODE must be parsed as a base-8 string and then security-validated; parsing does not imply acceptance. Accept only values in 0000..0777 with owner read/write, no execute bits, and no world permissions (for example 0600, 0640, and 0660); reject negative, special-bit, executable, or world-accessible modes (for example -1, 1660, 0666, and 0777). Production uses 0660.

- [ ] **Step 5: Run quality gates**

```bash
uv lock
uv sync --frozen
uv run pytest tests/unit/test_settings.py -v
uv run ruff check .
uv run mypy src integrations
```

Expected: all commands exit `0`.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock .gitignore README.md src tests
git commit -m "chore: scaffold isolated SEO orchestrator"
```

---

### Task 2: Canonical Domain Models and Fingerprints

**Files:**
- Create: `src/seo_orchestrator/canonical.py`
- Create: `src/seo_orchestrator/errors.py`
- Create: `src/seo_orchestrator/domain/models.py`
- Test: `tests/unit/test_canonical.py`
- Test: `tests/unit/test_domain_models.py`

**Interfaces:**
- Produces: `canonical_json(value: JsonValue) -> bytes`
- Produces: `sha256_fingerprint(value: JsonValue) -> str`
- Produces: Pydantic models `CompanyProfile`, `BusinessDirection`, `AudienceSegment`, `SeoBrief`, `ExecutionSnapshot`

- [ ] **Step 1: Write canonicalization failure tests**

Tests must prove sorted Unicode keys, stable list order, UTC timestamps, and fail-closed rejection of floats, bytes, sets, arbitrary objects, and non-string dict keys.

```python
def test_fingerprint_is_key_order_independent():
    assert sha256_fingerprint({"б": 2, "а": 1}) == sha256_fingerprint({"а": 1, "б": 2})


@pytest.mark.parametrize("value", [1.5, b"x", {"x"}, {1: "x"}, object()])
def test_canonical_json_rejects_non_contract_values(value):
    with pytest.raises(TypeError):
        canonical_json(value)
```

- [ ] **Step 2: Verify RED**

```bash
uv run pytest tests/unit/test_canonical.py -v
```

Expected: import failure.

- [ ] **Step 3: Implement canonical JSON**

Use `json.dumps(..., ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)` after a recursive allowlist validator accepting only `None`, `bool`, `int`, `str`, lists, and string-key dictionaries. Normalize timestamps to explicit UTC strings before canonicalization.

- [ ] **Step 4: Write exact domain validation tests**

Cover:

- positive integer versions;
- immutable IDs matching `^[a-z0-9][a-z0-9-]{1,63}$`;
- non-empty names and category context;
- direction company/version ownership fields;
- audience direction ownership fields;
- exactly one current-page source;
- explicit language and locale;
- deduplicated normalized competitor URLs.

- [ ] **Step 5: Implement Pydantic models**

Use frozen models and the exact fields from design spec Section 6 plus the mandatory version keys in this plan's Frozen Shared Contracts. Define IDs as `Annotated[str, StringConstraints(pattern=...)]`. Do not put secrets, credentials, provider responses, or mutable database handles on domain models.

- [ ] **Step 6: Verify and commit**

```bash
uv run pytest tests/unit/test_canonical.py tests/unit/test_domain_models.py -v
uv run ruff check src tests
uv run mypy src
git add src/seo_orchestrator/canonical.py src/seo_orchestrator/errors.py src/seo_orchestrator/domain tests/unit
git commit -m "feat: define canonical SEO domain contracts"
```

---

### Task 3: SQLite Schema, Migrations, and Transaction Boundaries

**Files:**
- Create: `src/seo_orchestrator/db/connection.py`
- Create: `src/seo_orchestrator/db/migrations.py`
- Create: `tests/integration/test_migrations.py`
- Create: `tests/integration/test_sqlite_concurrency.py`

**Interfaces:**
- Produces: `connect(path: Path) -> sqlite3.Connection`
- Produces: `transaction(conn) -> ContextManager[sqlite3.Connection]`
- Produces: `migrate(conn) -> int`

- [ ] **Step 1: Write RED migration tests**

Assert:

```python
assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
assert migrate(conn) == 1
assert migrate(conn) == 1  # idempotent
```

Also assert a process can read while another transaction writes, and that `busy_timeout` is non-zero.

- [ ] **Step 2: Define migration 0001**

Create tables:

```text
companies
company_profile_versions
business_direction_versions
audience_segment_versions
brief_drafts
execution_snapshots
jobs
job_transitions
approval_records
artifact_manifests
webhook_nonces
schema_migrations
```

Required constraints:

- composite uniqueness on `(company_id, version)` and equivalent direction/audience versions;
- foreign keys from direction to company profile version;
- audience to exact direction version;
- brief to exact company/direction/audience versions;
- snapshot hash unique;
- one active job per `created_by` using a partial unique index over active states;
- append-only transition IDs;
- no `ON DELETE CASCADE` from historical jobs or snapshots.

- [ ] **Step 3: Implement migration runner and transaction helper**

Set:

```sql
PRAGMA foreign_keys=ON;
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=5000;
```

Use `BEGIN IMMEDIATE` for version allocation and state transitions.

- [ ] **Step 4: Verify and commit**

```bash
uv run pytest tests/integration/test_migrations.py tests/integration/test_sqlite_concurrency.py -v
uv run ruff check src tests
uv run mypy src
git add src/seo_orchestrator/db tests/integration
git commit -m "feat: add durable SQLite schema and migrations"
```

---

### Task 4: Scoped Repositories and Cross-Company Isolation

**Files:**
- Create: `src/seo_orchestrator/db/repositories.py`
- Create: `tests/integration/test_repository_isolation.py`
- Create: `tests/security/test_cross_company_access.py`

**Interfaces:**
- Produces: `CompanyRepository`, `BriefRepository`, `SnapshotRepository`, `JobRepository`, `ApprovalRepository`
- Every child lookup consumes parent scope, for example `get_direction(company_id, direction_id, version)`.

- [ ] **Step 1: Write adversarial isolation tests**

Create Autobody and Confectionery records. Prove these calls fail with `NotFound` or `OwnershipMismatch`:

```python
repo.get_direction("avtomalyar", "wedding-cakes", 1)
repo.get_audience("sweet-world", "car-painting", "private-car-owners", 1)
repo.get_job("sweet-world", avtomalyar_job_id)
```

- [ ] **Step 2: Verify RED**

```bash
uv run pytest tests/integration/test_repository_isolation.py tests/security/test_cross_company_access.py -v
```

- [ ] **Step 3: Implement scoped SQL only**

Every query must include `company_id`. Direction queries also include `direction_id`; audience queries include both direction and audience identifiers. Do not implement unscoped `get_by_id()` methods for tenant-owned records.

- [ ] **Step 4: Verify query-plan indexes and commit**

Use `EXPLAIN QUERY PLAN` assertions to ensure composite indexes are used for scoped lookups.

```bash
uv run pytest tests/integration/test_repository_isolation.py tests/security/test_cross_company_access.py -v
uv run ruff check src tests
uv run mypy src
git add src/seo_orchestrator/db/repositories.py tests
git commit -m "feat: enforce company-scoped persistence"
```

---

### Task 5: Versioned Company Card Service

**Files:**
- Create: `src/seo_orchestrator/services/company_cards.py`
- Create: `tests/unit/test_company_cards.py`
- Create: `tests/integration/test_company_card_versions.py`
- Create: `fixtures/companies/avtomalyar.json`
- Create: `fixtures/companies/sweet-world.json`

**Interfaces:**
- Produces: `create_company(command: CreateCompany) -> CompanyProfile`
- Produces: `revise_company(command: ReviseCompany) -> CompanyProfile`
- Produces: `create_direction(command: CreateDirection) -> BusinessDirection`
- Produces: `revise_direction(command: ReviseDirection) -> BusinessDirection`
- Produces: `create_audience(command: CreateAudience) -> AudienceSegment`
- Produces: `archive_company(company_id, actor_id) -> None`

- [ ] **Step 1: Write versioning tests**

Prove revisions allocate monotonically increasing versions inside one transaction and never update prior rows. Prove archived companies remain readable by historical job scope but cannot start new drafts.

- [ ] **Step 2: Verify RED**

```bash
uv run pytest tests/unit/test_company_cards.py tests/integration/test_company_card_versions.py -v
```

- [ ] **Step 3: Implement commands and service**

Commands carry `actor_id`, expected current version, and exact replacement data. Reject stale edits with `VERSION_CONFLICT`; do not merge unspecified values into an approved version.

- [ ] **Step 4: Add deterministic fixtures**

Fixtures must contain invented non-secret data for:

- `avtomalyar` → `car-painting` → `private-car-owners`;
- `sweet-world` → `wedding-cakes` → `newlyweds`.

Do not copy client confidential text from n8n.

- [ ] **Step 5: Verify and commit**

```bash
uv run pytest tests/unit/test_company_cards.py tests/integration/test_company_card_versions.py -v
uv run ruff check src tests
uv run mypy src
git add src tests fixtures
git commit -m "feat: add versioned multi-company cards"
```

---

### Task 6: Resumable Brief Drafts and Execution Snapshot Compiler

**Files:**
- Create: `src/seo_orchestrator/services/briefs.py`
- Create: `src/seo_orchestrator/services/snapshots.py`
- Create: `tests/unit/test_brief_service.py`
- Create: `tests/integration/test_snapshot_compiler.py`

**Interfaces:**
- Produces: `start_brief(actor_id, company_id) -> SeoBriefDraft`
- Produces: `update_brief(command: UpdateBrief) -> SeoBriefDraft`
- Produces: `validate_brief(brief_id, actor_id) -> ValidatedBrief`
- Produces: `compile_snapshot(brief_id, prompt_set_version) -> ExecutionSnapshot`

- [ ] **Step 1: Write clearing-rule tests**

Assert changing company clears direction, audience, inherited structure, and category context. Changing direction clears audience and direction-derived fields. Assert a mismatched direction or audience fails before snapshot creation.

- [ ] **Step 2: Write immutable snapshot tests**

Compile snapshot A, revise the company, and prove snapshot A bytes/hash remain unchanged. Compile snapshot B and prove its hash differs. Retry from old job must resolve snapshot A by ID.

- [ ] **Step 3: Verify RED**

```bash
uv run pytest tests/unit/test_brief_service.py tests/integration/test_snapshot_compiler.py -v
```

- [ ] **Step 4: Implement normalized compiled context**

The compiled payload must use this top-level contract:

```json
{
  "schema_version": 1,
  "company": {},
  "direction": {},
  "audience": {},
  "brief": {},
  "prompt_set_version": 1
}
```

Store the exact canonical JSON bytes and SHA-256 hash in one transaction. Never reconstruct an existing snapshot from current profile rows.

- [ ] **Step 5: Verify and commit**

```bash
uv run pytest tests/unit/test_brief_service.py tests/integration/test_snapshot_compiler.py -v
uv run ruff check src tests
uv run mypy src
git add src tests
git commit -m "feat: compile immutable execution snapshots"
```

---

### Task 7: Approval Fingerprints and Job State Machine

**Files:**
- Create: `src/seo_orchestrator/domain/approvals.py`
- Create: `src/seo_orchestrator/domain/jobs.py`
- Create: `src/seo_orchestrator/services/approvals.py`
- Create: `src/seo_orchestrator/services/jobs.py`
- Create: `tests/unit/test_job_transitions.py`
- Create: `tests/integration/test_approval_invalidation.py`

**Interfaces:**
- Produces: `plan_job(snapshot_id, execution_plan) -> PlannedJob`
- Produces: `approve_job(job_id, actor_id, snapshot_hash, plan_fingerprint) -> ApprovalRecord`
- Produces: `transition(job_id, expected_state, target_state, reason) -> SeoJob`

- [ ] **Step 1: Encode allowed transitions in tests**

Use the exact state table from spec. Test every allowed edge and every forbidden edge. Terminal states never return to running. Export failure leaves content status successful.

- [ ] **Step 2: Write approval invalidation tests**

Approval must fail if any of these differ: snapshot hash, prompt set, model plan, provider plan, maximum retries, destination, or cost envelope version.

- [ ] **Step 3: Verify RED**

```bash
uv run pytest tests/unit/test_job_transitions.py tests/integration/test_approval_invalidation.py -v
```

- [ ] **Step 4: Implement compare-and-swap transitions**

Use one `BEGIN IMMEDIATE` transaction to check current state, update job, and append transition. A stale caller receives `STATE_CONFLICT`; duplicate cancel is idempotent.

- [ ] **Step 5: Verify and commit**

```bash
uv run pytest tests/unit/test_job_transitions.py tests/integration/test_approval_invalidation.py -v
uv run ruff check src tests
uv run mypy src
git add src tests
git commit -m "feat: enforce approvals and durable job states"
```

---

### Task 8: Immutable Artifact Store and Provenance Manifest

**Files:**
- Create: `src/seo_orchestrator/services/artifacts.py`
- Create: `tests/unit/test_artifact_manifest.py`
- Create: `tests/security/test_artifact_paths.py`
- Create: `fixtures/executions/success-result.json`

**Interfaces:**
- Produces: `write_bundle(job: SeoJob, result: ExecutionResult) -> ArtifactManifest`
- Produces: `open_artifact(company_id, job_id, name) -> BinaryIO`

- [ ] **Step 1: Write path security tests**

Reject absolute paths, `..`, alternate separators, symlink escapes, Unicode-confusable traversal, and cross-company job IDs. Permit only `content.md`, `metadata.json`, `qa.json`, `sources.json`, and `manifest.json`.

- [ ] **Step 2: Write atomicity tests**

Simulate failure before rename. Prove no partial final directory exists. Repeating `write_bundle` with the same result is idempotent; a different result for the same terminal job fails closed.

- [ ] **Step 3: Implement content-addressed staging**

Write into a same-filesystem temporary directory, `fsync` files and directory, validate all JSON, then atomically rename to:

```text
<artifact_root>/companies/<company_id>/jobs/<job_id>/
```

- [ ] **Step 4: Verify and commit**

```bash
uv run pytest tests/unit/test_artifact_manifest.py tests/security/test_artifact_paths.py -v
uv run ruff check src tests
uv run mypy src
git add src tests fixtures
git commit -m "feat: persist immutable SEO artifact bundles"
```

---

### Task 9: Worker API over ASGI and Unix Socket

**Files:**
- Create: `src/seo_orchestrator/api/auth.py`
- Create: `src/seo_orchestrator/api/app.py`
- Create: `src/seo_orchestrator/api/company_routes.py`
- Create: `src/seo_orchestrator/api/brief_routes.py`
- Create: `src/seo_orchestrator/api/job_routes.py`
- Create: `src/seo_orchestrator/api/callback_routes.py`
- Create: `src/seo_orchestrator/api/artifact_routes.py`
- Create: `src/seo_orchestrator/cli.py`
- Create: `tests/contract/test_worker_api.py`
- Create: `tests/security/test_api_auth.py`

**Interfaces:**
- Produces HTTP routes under `/v1` for company cards, briefs, plans, approvals, jobs, cancellation, retry, and artifacts.
- Produces CLI commands `seo-orchestrator migrate`, `serve`, `worker`, and `doctor`.

- [ ] **Step 1: Write API contract tests with `httpx.ASGITransport`**

Required behavior:

```text
GET  /v1/health                       -> 200 without auth, no sensitive detail
GET  /v1/companies                    -> 401 without bearer token
POST /v1/companies                    -> 201
POST /v1/briefs                       -> 201
POST /v1/briefs/{id}/validate         -> 200
POST /v1/jobs/plan                    -> 201
POST /v1/jobs/{id}/approve            -> 200
POST /v1/jobs/{id}/cancel             -> 200 idempotently
GET  /v1/jobs/{id}                    -> 200 company-scoped
GET  /v1/jobs/{id}/artifacts/content  -> 200 only after success
POST /v1/callbacks/n8n                -> 202 after valid HMAC and correlation
```

- [ ] **Step 2: Write auth tests**

Use a 32-byte random local API token loaded from a `0600` file. Compare with `hmac.compare_digest`. Reject missing, wrong, malformed, and oversized authorization headers. Health response contains only `{ "status": "ok" }`.

- [ ] **Step 3: Verify RED**

```bash
uv run pytest tests/contract/test_worker_api.py tests/security/test_api_auth.py -v
```

- [ ] **Step 4: Implement route adapters**

Routes perform schema parsing and call services; they contain no SQL or state-transition logic. Public errors return stable `code`, `message`, and `request_id`, never tracebacks.

- [ ] **Step 5: Implement Unix socket serve command**

Run Uvicorn with `uds=settings.socket_path`. Before bind, remove only an existing socket owned by UID `10000`; refuse regular files and foreign-owned sockets. After bind, set mode `0660`.

- [ ] **Step 6: Verify and commit**

```bash
uv run pytest tests/contract/test_worker_api.py tests/security/test_api_auth.py -v
uv run ruff check src tests
uv run mypy src
git add src tests
git commit -m "feat: expose authenticated worker API"
```

---

### Task 10: Durable Runner, Mock Executor, Retry, Resume, and Cancel

**Files:**
- Create: `src/seo_orchestrator/executors/base.py`
- Create: `src/seo_orchestrator/executors/mock.py`
- Create: `src/seo_orchestrator/runner.py`
- Create: `tests/unit/test_retry_policy.py`
- Create: `tests/integration/test_runner_recovery.py`
- Create: `tests/integration/test_runner_cancel.py`

**Interfaces:**
- Produces protocol `Executor.submit(job, snapshot) -> ExternalRun`
- Produces protocol `Executor.poll(run) -> ExecutionStatus`
- Produces protocol `Executor.cancel(run) -> None`
- Produces: `Runner.tick(limit: int = 1) -> int`

- [ ] **Step 1: Write deterministic executor and retry tests**

Script mock outcomes as `RUNNING`, `SUCCEEDED`, `429`, `503`, `MODEL_OUTPUT_INVALID`, and cancellation. Retry only transient network/429/502/503/504; never retry validation/schema/approval failures. Assert exponential delays with injected clock and jitter function.

- [ ] **Step 2: Write crash recovery tests**

Stop runner after external submission but before local status update. Restart and prove idempotency key prevents duplicate execution. Mark stale running jobs for reconciliation, not blind resubmission.

- [ ] **Step 3: Verify RED**

```bash
uv run pytest tests/unit/test_retry_policy.py tests/integration/test_runner_recovery.py tests/integration/test_runner_cancel.py -v
```

- [ ] **Step 4: Implement one-tick runner**

Keep `tick()` bounded and testable. The long-running CLI loop repeatedly calls `tick()`, records heartbeat, handles SIGTERM, and exits after the current atomic transition. Never hold a database transaction during network I/O.

- [ ] **Step 5: Verify and commit**

```bash
uv run pytest tests/unit/test_retry_policy.py tests/integration/test_runner_recovery.py tests/integration/test_runner_cancel.py -v
uv run ruff check src tests
uv run mypy src
git add src tests
git commit -m "feat: add recoverable SEO job runner"
```

---

### Task 11: URL Policy and SSRF Guard

**Files:**
- Create: `src/seo_orchestrator/security/url_policy.py`
- Create: `tests/security/test_url_policy.py`
- Create: `tests/security/test_redirect_policy.py`

**Interfaces:**
- Produces: `normalize_public_http_url(raw: str, resolver: Resolver) -> NormalizedUrl`
- Produces: `validate_redirect(source, target, resolver) -> NormalizedUrl`

- [ ] **Step 1: Write bypass corpus tests**

Reject:

```text
file:///etc/passwd
http://127.0.0.1
http://[::1]
http://169.254.169.254
http://0x7f000001
http://2130706433
http://localhost
http://user:pass@example.com
http://example.com#secret
```

Reject DNS answers containing private, loopback, link-local, multicast, reserved, or unspecified addresses. Revalidate every redirect destination and resolved address.

- [ ] **Step 2: Verify RED**

```bash
uv run pytest tests/security/test_url_policy.py tests/security/test_redirect_policy.py -v
```

- [ ] **Step 3: Implement pure policy with injectable resolver**

Use `urllib.parse`, `ipaddress`, IDNA normalization, explicit ports, and an injected resolver. Do not perform live DNS in unit tests. Preserve normalized public URL without fragments or userinfo.

- [ ] **Step 4: Verify and commit**

```bash
uv run pytest tests/security/test_url_policy.py tests/security/test_redirect_policy.py -v
uv run ruff check src tests
uv run mypy src
git add src tests
git commit -m "feat: block unsafe SEO source URLs"
```

---

### Task 12: Signed n8n Executor and Mock Wrapper Contract

**Files:**
- Create: `src/seo_orchestrator/security/signatures.py`
- Create: `src/seo_orchestrator/executors/n8n.py`
- Create: `integrations/n8n/universal-contract.json`
- Create: `tests/contract/test_n8n_signatures.py`
- Create: `tests/contract/test_n8n_executor.py`
- Create: `tests/contract/mock_n8n_app.py`

**Interfaces:**
- Produces: `sign_request(method, path, timestamp, nonce, body, key) -> str`
- Produces: `verify_request(...) -> None`
- Produces: `N8nExecutor(base_url, key, client)` implementing `Executor`

- [ ] **Step 1: Freeze signature format in tests**

Canonical signing input:

```text
METHOD\nPATH\nUNIX_TIMESTAMP\nNONCE\nSHA256_HEX(BODY)
```

Header names:

```text
X-SEO-Timestamp
X-SEO-Nonce
X-SEO-Idempotency-Key
X-SEO-Signature
```

Use HMAC-SHA256 hex. Reject timestamp skew greater than 300 seconds and replayed nonce.

- [ ] **Step 2: Write executor contract tests**

Assert submit body contains `job_id`, `brief_fingerprint`, `snapshot_hash`, and complete `ExecutionSnapshot`; status and cancel requests are signed; result parser rejects missing five titles/descriptions, invalid status, hidden reasoning fields, and oversized payloads.

- [ ] **Step 3: Verify RED**

```bash
uv run pytest tests/contract/test_n8n_signatures.py tests/contract/test_n8n_executor.py -v
```

- [ ] **Step 4: Implement with injected `httpx.Client`**

Set explicit connect/read/write/pool timeouts. Disable automatic redirects. Never log body or signing key. Map n8n responses to stable worker error codes.

- [ ] **Step 5: Verify and commit**

```bash
uv run pytest tests/contract/test_n8n_signatures.py tests/contract/test_n8n_executor.py -v
uv run ruff check src tests
uv run mypy src
git add src integrations/n8n tests/contract
git commit -m "feat: add signed universal n8n executor"
```

---

### Task 13: Hermes Profile-Local Plugin against Worker API

**Files:**
- Create: `integrations/hermes/seo_orchestrator/plugin.yaml`
- Create: `integrations/hermes/seo_orchestrator/__init__.py`
- Create: `integrations/hermes/seo_orchestrator/client.py`
- Create: `integrations/hermes/seo_orchestrator/schemas.py`
- Create: `integrations/hermes/seo_orchestrator/tools.py`
- Create: `tests/contract/test_hermes_plugin.py`
- Create: `tests/security/test_hermes_plugin_outputs.py`

**Interfaces:**
- Registers toolset `seo_orchestrator` with no built-in overrides.
- Handler signature: `handler(args: dict[str, Any], **kwargs: Any) -> str`.
- Client talks to `/opt/data/seo-runtime/worker.sock` using stdlib Unix-socket HTTP and bearer token file.

- [ ] **Step 1: Write plugin registration tests**

Use a fake `ctx` and assert exact tools are registered:

```text
seo_company_list
seo_company_get
seo_company_save_draft
seo_brief_start
seo_brief_update
seo_brief_validate
seo_job_plan
seo_job_approve
seo_job_status
seo_job_cancel
seo_job_retry
seo_job_artifact
seo_export_approve
```

Assert `override=False`, `toolset="seo_orchestrator"`, JSON schemas set `additionalProperties: false`, and mutating tools require explicit identifiers and expected versions.

- [ ] **Step 2: Write Unix-socket client tests**

Create a temporary Unix HTTP server. Prove bearer auth, timeout, response-size cap, JSON-only response, and redaction of authorization headers from errors.

- [ ] **Step 3: Verify RED**

```bash
uv run pytest tests/contract/test_hermes_plugin.py tests/security/test_hermes_plugin_outputs.py -v
```

- [ ] **Step 4: Implement plugin manifest and registration**

`plugin.yaml`:

```yaml
name: seo-orchestrator
version: 0.1.0
description: "Control durable multi-company SEO jobs through an isolated local worker."
author: Verexela
kind: standalone
platforms:
  - linux
provides_tools:
  - seo_company_list
  - seo_company_get
  - seo_company_save_draft
  - seo_brief_start
  - seo_brief_update
  - seo_brief_validate
  - seo_job_plan
  - seo_job_approve
  - seo_job_status
  - seo_job_cancel
  - seo_job_retry
  - seo_job_artifact
  - seo_export_approve
```

`register(ctx)` calls `ctx.register_tool(...)` for each tool. Do not register hooks, slash commands, tool overrides, or message injection in MVP.

- [ ] **Step 5: Verify and commit**

```bash
uv run pytest tests/contract/test_hermes_plugin.py tests/security/test_hermes_plugin_outputs.py -v
uv run ruff check integrations tests
uv run mypy integrations
git add integrations/hermes tests
git commit -m "feat: add narrow Hermes SEO control plugin"
```

**Gate G2:** Stop here before copying or enabling the plugin in any Hermes profile.

---

### Task 14: Fixed Progress Notifications and Deliver-Only Sink

**Files:**
- Create: `src/seo_orchestrator/services/notifications.py`
- Create: `tests/unit/test_notifications.py`
- Create: `tests/contract/test_hermes_delivery_sink.py`

**Interfaces:**
- Produces: `NotificationEvent`
- Produces: `NotificationSink.send(event) -> DeliveryReceipt`
- Produces: `HermesWebhookSink` with HMAC-authenticated deliver-only payloads.

- [ ] **Step 1: Write content-minimization tests**

Notifications may contain job ID, company display name, direction display name, completed stage count, current stage, retry state, elapsed duration, and artifact-ready flag. Reject prompts, scraped text, credentials, model reasoning, full provider output, and raw source bodies.

- [ ] **Step 2: Write deduplication tests**

Use `(job_id, event_type, stage, attempt)` as event key. Repeated delivery after a timeout must not create duplicate user-visible progress. Completion is delivered exactly once per destination.

- [ ] **Step 3: Implement sink behind disabled configuration**

Default configuration uses `NullNotificationSink`. Live webhook URL, HMAC key, and destination are accepted only from secret files/config, never from company profiles.

- [ ] **Step 4: Verify and commit**

```bash
uv run pytest tests/unit/test_notifications.py tests/contract/test_hermes_delivery_sink.py -v
uv run ruff check src tests
uv run mypy src
git add src tests
git commit -m "feat: add privacy-bounded SEO progress events"
```

**Gate G5:** Stop before configuring a live Telegram/Hermes destination or sending any message.

---

### Task 15: Local End-to-End MVP with Two Companies and Zero n8n Mutations

**Files:**
- Create: `tests/e2e/test_two_company_flow.py`
- Create: `tests/e2e/test_restart_resume_flow.py`
- Create: `tests/e2e/test_approval_change_flow.py`

**Interfaces:**
- Exercises worker API, SQLite, runner, mock executor, artifacts, and notification sink in one process.

- [ ] **Step 1: Write two-company E2E test**

Execute sequential jobs:

```text
avtomalyar / car-painting / private-car-owners
sweet-world / wedding-cakes / newlyweds
```

Assert:

- same executor implementation and same pipeline version;
- different snapshot hashes;
- no field from either company appears in the other artifact manifest;
- company CRUD causes zero calls to executor configuration or workflow mutation APIs;
- both job directories are company-scoped.

- [ ] **Step 2: Write restart and approval tests**

Restart app and runner between `QUEUED` and `RUNNING`, then between `RUNNING` and `SUCCEEDED`. Change a direction after plan creation and prove old approval cannot start a new snapshot.

- [ ] **Step 3: Run complete local suite**

```bash
uv run pytest tests -v
uv run ruff check .
uv run mypy src integrations
```

Expected: all tests pass; no network access is required.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e
git commit -m "test: prove isolated two-company SEO flow"
```

---

### Task 16: Deterministic Local n8n Workflow Parameterizer

**Files:**
- Create: `integrations/n8n/source-workflow.json`
- Create: `integrations/n8n/expected-node-map.json`
- Create: `integrations/n8n/transform.py`
- Create: `integrations/n8n/validate.py`
- Create: `tests/contract/test_workflow_transform.py`
- Create: `tests/contract/test_workflow_invariants.py`

**Interfaces:**
- Produces: `transform_workflow(source: dict) -> dict`
- Produces: `validate_universal_workflow(workflow: dict) -> ValidationReport`
- Produces no n8n network calls.

- [ ] **Step 1: Sanitize and freeze source fixture**

Copy the saved read-only workflow structure from `/opt/data/tmp/n8n_first_workflow_details.json` while replacing credential identifiers, document IDs, sheet IDs, URLs containing tokens, and credentials with `[REDACTED]`. Preserve node IDs, names, types, positions, parameters needed for transformation, and connections. Add a sanitizer test that rejects known credential field names and secret patterns.

- [ ] **Step 2: Write source invariants**

Require exact presence and type for:

```text
When clicking ‘Execute workflow’
Для кого пишем
SEO ТЗ из таблицы
Цикл
Оценщик
Финальный вывод
```

Require profile-dependent nodes:

```text
Оценщик
Генерация мета-тегов
БРИФ ДЛЯ ОБОГАЩЕНИЯ КОНТЕНТА СТРАНИЦЫ
Редактура и проверка вписывания ссылок
Писатель
Поиск исследований
```

Fail closed if source node names/types or critical connections differ from the expected map.

- [ ] **Step 3: Write transformation tests**

The output must:

- preserve all original node IDs except explicitly removed disconnected nodes;
- preserve the source object unchanged;
- replace manual and source-sheet ingress with one validated snapshot ingress;
- parameterize `company`, `target`, `stucture`, `person`, `podrajanie`, `language`, `LOCALE`, and `year` from the current snapshot/prompt set;
- remove ResultUP literals from executable parameters;
- remove source reads `SEO ТЗ из таблицы`, `Сбор кейсов`, `Ссылки для линковки1` only after equivalent snapshot fields are connected;
- replace `Финальный вывод` Sheets update with structured result output;
- remove disconnected OpenAI/Supabase/Cohere cluster and the two isolated Firecrawl nodes;
- keep main processing order from `Тип контекста` through `Оценщик`;
- contain no pinned data;
- contain no company-specific fallback;
- include pipeline version and snapshot hash in output.

- [ ] **Step 4: Verify RED**

```bash
uv run pytest tests/contract/test_workflow_transform.py tests/contract/test_workflow_invariants.py -v
```

- [ ] **Step 5: Implement pure transformer and validator**

Use exact node names and assert before mutation. Deep-copy input. Emit a deterministic JSON file with sorted keys only for local review; n8n import ordering may remain source-compatible.

- [ ] **Step 6: Verify and commit**

```bash
uv run pytest tests/contract/test_workflow_transform.py tests/contract/test_workflow_invariants.py -v
uv run ruff check integrations tests
uv run mypy integrations
git add integrations/n8n tests/contract
git commit -m "feat: parameterize universal n8n workflow locally"
```

No n8n workflow has been created or changed at this point.

---

### Task 17: Approved n8n Cloud Integration and Canary

**Files:**
- Create after approval: `docs/evidence/n8n-stage-b-canary.md`
- Modify after approval: approved n8n Cloud workspace only
- Do not store exported credentials or tokens in the repository.

**Interfaces:**
- Produces one protected wrapper and one universal workflow copy.
- Produces an immutable external workflow ID recorded in deployment config, not hardcoded in domain data.

**Gate G3:** Obtain explicit approval before any step in this task.

- [ ] **Step 1: Present least-privilege change set**

Request only the capability necessary to create/import and operate the dedicated copy. Prefer a dedicated signed webhook over broad generic `workflow:execute`. Show exact scopes, webhook exposure, rollback, and any n8n plan limitations before authorization.

- [ ] **Step 2: Create rollback evidence**

Record original workflow ID, status, version metadata, and hash of the sanitized structure. Confirm original remains inactive and unchanged.

- [ ] **Step 3: Create the isolated universal copy**

Import the locally validated transformed workflow under a new name containing `UNIVERSAL STAGE B`. Do not edit the original. Configure credentials by selecting existing n8n credentials without reading or exporting secret values.

- [ ] **Step 4: Create signed wrapper endpoints**

Expose only submit/status/cancel for the dedicated copy. Validate body schema, HMAC, timestamp, nonce, idempotency key, and snapshot hash before execution. Persist accepted nonces and idempotency keys in a dedicated n8n Data Table named `seo_wrapper_requests`, with unique constraints on nonce and idempotency key and a retention timestamp. This table contains request identifiers and hashes only—never company profile data, prompts, or credentials.

- [ ] **Step 5: Run a free/mock contract probe**

Use a mock execution mode that returns fixture output and causes no paid provider calls. Verify worker submit/status/result and n8n execution ID correlation.

**Gate G4:** Obtain explicit approval before the next step.

- [ ] **Step 6: Run one paid canary**

Display company, direction, audience, snapshot hash, provider/model plan, maximum call count, current quota/tariff information, and cost uncertainty. Run one approved canary only.

- [ ] **Step 7: Verify evidence and rollback**

Document execution ID, timings, stage statuses, artifact hashes, warnings, and cost evidence without secrets. On failure, deactivate the wrapper/copy; do not modify the original.

- [ ] **Step 8: Commit evidence only**

```bash
git add docs/evidence/n8n-stage-b-canary.md
git commit -m "docs: record Stage B n8n canary evidence"
```

---

### Task 18: Optional Google Sheets Export Adapter

**Files:**
- Create: `src/seo_orchestrator/services/exports.py`
- Create: `tests/contract/test_sheet_export.py`
- Create: `tests/integration/test_export_approval.py`

**Interfaces:**
- Produces: `prepare_export(job_id, target) -> ExportPlan`
- Produces: `approve_export(job_id, plan_fingerprint, actor_id) -> ApprovalRecord`
- Produces: `execute_export(approval_id) -> ExportReceipt`

- [ ] **Step 1: Write approval-boundary tests**

Generation success must not write Sheets. Export requires a separate approval bound to spreadsheet, sheet/tab, row selector, columns, and artifact hash. Destination change invalidates approval.

- [ ] **Step 2: Write idempotent update tests**

Use a deterministic export key from job ID and artifact hash. Duplicate callback or retry must not append duplicate rows. Reject formulas beginning with `=`, `+`, `-`, or `@` when exporting user-controlled text to CSV/Sheets cells unless explicitly escaped.

- [ ] **Step 3: Implement adapter interface with mock backend**

Implement the real export path only through a dedicated n8n export workflow using the existing n8n-managed Google credential. The worker stores no Google OAuth token and sends only the approved export payload and destination contract.

- [ ] **Step 4: Verify and commit local implementation**

```bash
uv run pytest tests/contract/test_sheet_export.py tests/integration/test_export_approval.py -v
uv run ruff check src tests
uv run mypy src
git add src tests
git commit -m "feat: gate optional Sheets export"
```

**Gate G6:** Obtain explicit approval before a real Sheets write.

---

### Task 19: Rootless Container Packaging and User Systemd Unit

**Files:**
- Create: `ops/Containerfile`
- Create: `ops/entrypoint.sh`
- Create: `ops/systemd/hermes-seo-orchestrator.service`
- Create: `ops/scripts/preflight.sh`
- Create: `tests/ops/test_container_contract.py`

**Interfaces:**
- Produces a worker image with no published ports.
- Produces a user systemd unit running UID/GID `10000:10000` with read-only root filesystem.

- [ ] **Step 1: Write static deployment contract tests**

Assert:

- image uses Python 3.13;
- non-root UID/GID 10000;
- root filesystem intended read-only;
- no `EXPOSE` instruction;
- secrets are file mounts, not environment values printed by entrypoint;
- health check uses Unix socket;
- service has CPU, memory, PID, restart, and timeout limits;
- worker data volume is separate from `hermes-guldan-data`;
- only `hermes-guldan-data` subpath `seo-runtime` is mounted to the worker runtime path;
- gateway requires no new TCP port or network.

- [ ] **Step 2: Implement preflight script**

The script must fail unless:

```text
current gateway UID/GID is 10000:10000
hermes-guldan-data volume exists
seo-runtime subpath exists and is owned by 10000:10000
worker data volume is writable by 10000:10000
socket path is absent or a socket owned by 10000
no container publishes worker TCP ports
```

It prints no environment or secret contents.

- [ ] **Step 3: Build and inspect image locally**

Run the available rootless Docker build command. Inspect user, mounts, read-only root, capabilities, network, and command. Do not start the live service.

- [ ] **Step 4: Verify image in an isolated test container**

Mount temporary data and runtime volumes, start with mock executor, run `doctor`, call health over Unix socket, create one mock job, stop with SIGTERM, restart, and verify state persists.

- [ ] **Step 5: Commit packaging**

```bash
git add ops tests/ops
git commit -m "ops: package isolated SEO worker"
```

**Gate G7:** Obtain explicit approval before creating persistent Docker volumes, installing/enabling systemd unit, or starting the deployed worker.

- [ ] **Step 6: Deploy after approval**

Create the dedicated worker data volume and `seo-runtime` subpath; install the user unit; start worker; verify Unix socket ownership/mode, health, restart behavior, no TCP listener, resource limits, and gateway responsiveness.

- [ ] **Step 7: Install Hermes plugin only after G2 approval**

Copy the reviewed plugin directory to `/opt/data/plugins/seo_orchestrator`, run `/opt/hermes/.venv/bin/hermes plugins enable --no-allow-tool-override seo-orchestrator`, run `/opt/hermes/.venv/bin/hermes plugins list --json`, then perform the required graceful gateway restart/reload. Verify tools appear and no built-in tool is overridden.

---

### Task 20: Stage B Release Benchmark and Stage C Entry Gate

**Files:**
- Create: `fixtures/benchmark/manifest.json`
- Create: `tests/e2e/test_benchmark_manifest.py`
- Create: `docs/evidence/stage-b-release-verdict.md`

**Interfaces:**
- Produces a reproducible benchmark manifest and explicit release verdict.

- [ ] **Step 1: Freeze 3–5 benchmark briefs**

Include at minimum:

- local automotive commercial service page;
- B2B complex service page;
- consumer confectionery page;
- one different locale/language if source workflow supports it.

Each case records exact company/direction/audience versions, snapshot hash, prompt set version, expected required sections, prohibited claims, and evaluator rubric.

- [ ] **Step 2: Run source and universal paths on equivalent inputs**

Do not compare exact wording. Compare required section coverage, factual support, source traceability, keyword naturalness, overuse, internal links, language/locale compliance, meta schema, human editorial score, paid-call count, runtime, and retry behavior.

- [ ] **Step 3: Run cross-company contamination checks**

Search artifacts and manifests for names, offers, cities, URLs, case references, and unique marker phrases belonging to every other fixture. Any contamination is a release-blocking failure.

- [ ] **Step 4: Run full verification suite**

```bash
uv sync --frozen
uv run pytest tests -v
uv run ruff check .
uv run mypy src integrations
```

Also run container contract, restart recovery, n8n wrapper contract, plugin discovery in isolated profile, and `git diff --check`.

- [ ] **Step 5: Write release verdict**

Verdict must be one of:

```text
APPROVED_FOR_SINGLE_USER_STAGE_B
REJECTED
```

List evidence, deviations, costs, risks, rollback, and unsupported cases. No conditional or ambiguous verdict.

- [ ] **Step 6: Commit evidence**

```bash
git add fixtures/benchmark tests/e2e/test_benchmark_manifest.py docs/evidence/stage-b-release-verdict.md
git commit -m "test: certify Stage B SEO orchestrator"
```

- [ ] **Step 7: Open Stage C migration planning only after approval verdict**

Create separate implementation plans in this order:

```text
1. deterministic transforms: n-grams, metrics, manifest assembly
2. URL fetch and content normalization
3. research adapter and source provenance
4. prompt registry and model routing
5. brief generation
6. writer and editor
7. keyword QA and meta evaluator
8. optional Sheets adapter
9. n8n retirement or integrations-only decision
```

Each Stage C plan must preserve the Stage B `ExecutionSnapshot`, `Executor`, result, artifact, and approval contracts and must pass the same benchmark before cutover.

---

## Plan Completion Checklist

- [ ] Every design-spec requirement maps to a task.
- [ ] Company CRUD produces zero n8n node mutations.
- [ ] Original n8n workflow remains untouched.
- [ ] Two-company contamination test exists before cloud integration.
- [ ] Local system works entirely with mock executor before external approval.
- [ ] Production uses Unix socket and no TCP listener.
- [ ] Plugin does not override built-in tools or inject messages.
- [ ] Paid execution, live Telegram delivery, Sheets write, and deployment have separate gates.
- [ ] Stage C starts only after a successful Stage B benchmark.
