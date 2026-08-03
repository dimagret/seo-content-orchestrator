# Telegram SEO Content Orchestrator — Design Specification

**Status:** Approved architecture; written-spec review pending  
**Date:** 2026-08-03  
**Decision:** Target architecture C (isolated orchestrator), introduced through stage B (n8n execution engine)  
**Owner:** Dmitry Gretchenko / Gul’dan

## 1. Goal

Build a durable SEO content-production system controlled through the existing Gul’dan Telegram conversation. The first release collects the full brief in Telegram, explicitly selects a company, business direction, and audience segment, requires approval before paid calls, executes an isolated copy of the current n8n workflow, and returns structured artifacts to Telegram with optional Google Sheets export. The target system progressively moves execution into an isolated, testable worker while keeping the live Hermes gateway responsive.

Functional parity means equivalent stages, inputs, checks, and output artifacts. It does not mean identical LLM wording or a one-to-one recreation of n8n nodes.

## 2. Approved Product Decisions

1. Telegram is the primary input and control surface.
2. Google Sheets is an optional output adapter, not the source of the brief.
3. Multiple companies, business directions, and audience segments are supported from the first MVP.
4. Gul’dan is the control plane; heavy work never executes in the live gateway process.
5. Stage B uses a protected n8n wrapper and an isolated copy of the workflow.
6. Stage C migrates stages into a dedicated `seo-content-orchestrator` worker behind stable contracts.
7. Paid generation, external writes, and final publication remain explicit approval gates.
8. The original n8n workflow is read-only evidence and is not edited in place.
9. Company creation, selection, and editing never creates, deletes, rewires, or rewrites n8n nodes.
10. One universal Stage B workflow copy serves all companies by receiving an explicit immutable `ExecutionSnapshot` on every job.
11. The orchestrator database, not n8n canvas state, is the source of truth for company cards and their versions.

## 3. Source Workflow Audit

Source workflow:

- ID: `0ORDwUdQfn3dkEut`
- Name: `Убийца копиров коммерция V2 (общая сборка с большим количеством комментов)`
- State: inactive, manual trigger
- Total nodes: 62
- Sticky notes: 15
- Main connected executable component: 42 non-sticky nodes
- Disconnected component: OpenAI embeddings, Supabase Vector Store, Cohere reranker
- Isolated nodes: one Firecrawl scrape node and one Firecrawl tool node

Observed execution metadata:

- 13 manual executions
- 6 `success`
- 5 `error`
- 2 `canceled`
- latest successful execution: 197.368 seconds

These counts are not an end-to-end reliability percentage because manual executions can run partial paths.

### 3.1 Active Logical Stages

1. Read SEO task data.
2. Resolve company, audience, locale, language, page structure, and page type.
3. Split and validate competitor URLs.
4. Scrape competitor and current-page content.
5. Normalize and aggregate competitor content.
6. Build a competitor-derived content brief with GPT-5.
7. Research facts and sources with Perplexity Sonar Pro.
8. Load cases and internal-link candidates.
9. Extract deterministic 2-grams and 3-grams.
10. Filter n-grams with Gemini 2.5 Flash.
11. Generate commercial page copy with GPT-4o.
12. Edit copy and insert internal links with GPT-4o.
13. Calculate deterministic text metrics.
14. Run keyword and LSI quality control with GPT-4o.
15. Generate title and description candidates with GPT-4o.
16. Score candidates and select five titles and five descriptions.
17. Persist the result.

### 3.2 Problems Not to Copy

- ResultUP data is hardcoded in the workflow.
- The workflow is coupled to specific Google Sheets layouts.
- Unused nodes make the canvas appear more capable than its executed graph.
- Prompts are embedded in nodes and are not independently versioned or tested.
- Model calls, deterministic transforms, integrations, and persistence are mixed in one graph.
- Manual-trigger execution has no stable API contract for Telegram.
- Execution metadata shows errors and cancellations, but stage-level failure reasons are not represented in the workflow contract.

## 4. Scope

### 4.1 MVP Scope

- Manage multiple companies with versioned business directions and audience segments.
- Collect and resume a full SEO brief in Telegram.
- Validate the brief before execution.
- Show a deterministic execution plan and estimated paid-call envelope.
- Require explicit approval before paid execution.
- Submit one idempotent job to a protected n8n wrapper.
- Track durable job status outside the Hermes gateway.
- Send progress and completion notifications through the existing Telegram channel.
- Return Markdown content, five titles, five descriptions, QA results, sources, warnings, and execution evidence.
- Offer Google Sheets export only after a separate write approval.
- Support cancel, retry from a safe checkpoint, and resume after worker restart.

### 4.2 Non-Goals for MVP

- Editing or replacing the original n8n workflow.
- Automatic publication to a website or CMS.
- Autonomous Google Sheets writes without approval.
- Billing, multi-tenant SaaS accounts, or external user onboarding.
- A visual workflow editor.
- Reintroducing the disconnected RAG component without measured quality benefit.
- Exact textual equality with the source workflow.
- Multiple concurrent jobs for the same Telegram user.

## 5. System Architecture

```text
Telegram / Gul’dan
        |
        | local Hermes plugin tools
        v
SEO Control Adapter
        |
        | authenticated local API over Unix domain socket
        v
SEO Orchestrator Worker + SQLite/WAL
        |
        | Stage B: signed, idempotent request
        v
Protected n8n Wrapper -> Isolated Workflow Copy
        |
        | structured result / status
        v
SEO Orchestrator Worker
        |
        +--> Hermes webhook deliver-only -> Telegram progress
        +--> local artifacts
        +--> optional Google Sheets adapter after approval
```

### 5.1 Isolation Boundary

The live Hermes gateway may validate commands, invoke local plugin tools, and display state. It must not scrape pages, call generation models, hold stage payloads in conversation context, or own durable job transitions.

The worker runs as a separate bounded process or service with its own resource limits. A worker crash must not stop Telegram. A gateway restart must not lose jobs.

Production transport is a Unix domain socket at `/opt/data/seo-runtime/worker.sock` as seen by the gateway. The worker container mounts only the `seo-runtime` subpath of the existing `hermes-guldan-data` volume at `/run/seo-orchestrator` and listens on `/run/seo-orchestrator/worker.sock`. The worker runs as UID/GID `10000:10000`, creates the socket with mode `0660`, and does not publish a TCP port. Tests may use ASGI in-process transport or loopback TCP; production must not.

### 5.2 Hermes Integration

Use a profile-local Hermes plugin under `$HERMES_HOME/plugins/`; do not edit Hermes core. The plugin exposes narrow JSON-returning tools:

- `seo_company_list`
- `seo_company_get`
- `seo_company_save_draft`
- `seo_brief_start`
- `seo_brief_update`
- `seo_brief_validate`
- `seo_job_plan`
- `seo_job_approve`
- `seo_job_status`
- `seo_job_cancel`
- `seo_job_retry`
- `seo_job_artifact`
- `seo_export_approve`

The plugin communicates only with the localhost worker API. It never receives n8n, model-provider, Firecrawl, or Google credentials.

Progress and completion use a Hermes HMAC webhook subscription in `deliver-only` mode. This avoids a second Telegram polling process and avoids an unnecessary LLM call for fixed status notifications.

### 5.3 Persistent Company Card Storage

The Stage B MVP stores company cards in the orchestrator's durable SQLite/WAL database. n8n Data Tables, pinned node data, workflow static data, Google Sheets, and prompt text are not the source of truth for company information.

The storage model contains separate records for:

- companies;
- company profile versions;
- business direction versions;
- audience segment versions;
- page brief drafts;
- immutable execution snapshots;
- jobs, approvals, transitions, and artifact manifests.

Telegram operations create or update database records only:

- create company;
- select company;
- show company card;
- create or edit a direction;
- create or edit an audience segment;
- clone a direction as a draft;
- archive a company without deleting historical jobs;
- compile a new execution snapshot.

None of these operations mutates n8n workflow topology. Editing approved profile information creates a new version; existing snapshots and historical jobs continue to reference their original versions.

The database is local to the isolated worker for MVP. A later PostgreSQL migration may replace SQLite behind the same repository contract if concurrent users or multiple worker replicas require it; this does not change the n8n payload contract.

## 6. Domain Model

The word **manual** means a compiled, immutable execution context. It is not one editable text field and is never global. The hierarchy is:

```text
Company
  -> CompanyProfile version
  -> BusinessDirection version
  -> AudienceSegment version
  -> PageBrief
  -> ExecutionSnapshot
  -> SeoJob
```

A company may have many directions. A direction may have many audience segments and page briefs. Every approved job references exact versions and a content hash; it never reads whichever company or direction happens to be selected later.

### 6.1 Company and CompanyProfile

`Company` is the client boundary. `CompanyProfile` contains facts stable across its directions.

Required fields:

- `company_id`
- `company_profile_id`
- `company_profile_version`
- `name`
- `brand_summary`
- `products_services_overview`
- `commercial_model`
- `pricing_overview`
- `service_geography`
- `value_propositions`
- `proof_points`
- `certifications`
- `case_references`
- `tools_and_process`
- `tone_of_voice`
- `positive_voice_examples`
- `negative_voice_examples`
- `reading_level`
- `allowed_claims`
- `forbidden_claims`
- `compliance_requirements`
- `default_language`
- `default_locale`
- `created_at`
- `updated_at`

This replaces the source workflow's unstructured `company`, `person`, `podrajanie`, `language`, `LOCALE`, and `year` values where they describe the company as a whole.

### 6.2 BusinessDirection

`BusinessDirection` is one product, service line, category, or market context inside a company. Autobody repair and car painting may be separate directions under one automotive company; confectionery is a direction under a different company.

Required fields:

- `direction_id`
- `company_id`
- `company_profile_version`
- `direction_version`
- `name`
- `offerings`
- `category_context`
- `prices_and_tariffs`
- `direction_value_propositions`
- `direction_proof_points`
- `direction_cases`
- `internal_link_catalog`
- `default_page_structure`
- `default_language`
- `default_locale`
- `allowed_claims`
- `forbidden_claims`
- `created_at`
- `updated_at`

This separates the source workflow's large `stucture` value and the per-category context that its manual says must be changed for different services.

### 6.3 AudienceSegment

An audience segment belongs to one business direction. It is not shared implicitly across companies.

Required fields:

- `audience_segment_id`
- `company_id`
- `direction_id`
- `audience_version`
- `name`
- `buyer_roles`
- `industry`
- `company_or_customer_size`
- `geography`
- `jobs_to_be_done`
- `pains_and_risks`
- `objections`
- `objection_responses`
- `selection_criteria`
- `minimum_expectations`
- `purchase_triggers`
- `budget_range`
- `decision_cycle`
- `decision_participants`
- `preferred_content_formats`
- `created_at`
- `updated_at`

This replaces the source workflow's unstructured `target` value.

### 6.4 SeoBrief

`SeoBrief` describes one page task and must explicitly select one company, one direction, and one audience version.

Required fields:

- `brief_id`
- `company_id`
- `company_profile_version`
- `direction_id`
- `direction_version`
- `audience_segment_id`
- `audience_version`
- `page_type`
- `goal`
- `target_language`
- `locale`
- `page_structure`
- `primary_keyword`
- `keywords`
- `lsi_terms`
- `competitor_urls`
- `current_page_url` or `current_page_context`
- `output_sheet_target` when export is requested
- `created_by`
- `created_at`
- `updated_at`

Validation rules:

- the direction must belong to the selected company;
- the audience segment must belong to the selected direction;
- all referenced versions must exist and be immutable;
- exactly one of `current_page_url` and `current_page_context` is required;
- competitor URLs must use `http` or `https` and pass SSRF policy;
- duplicate URLs are normalized and removed;
- language and locale are explicit;
- page structure cannot be empty;
- paid execution cannot begin from a draft brief.

### 6.5 ExecutionSnapshot

Before approval, the worker compiles an immutable execution manual from the selected versions and brief.

Required fields:

- `snapshot_id`
- `company_id`
- `company_profile_version`
- `direction_id`
- `direction_version`
- `audience_segment_id`
- `audience_version`
- `brief_id`
- `prompt_set_version`
- `compiled_context`
- `snapshot_hash`
- `created_at`

`compiled_context` is the complete normalized payload required by prompts. n8n receives this snapshot explicitly on every execution. It must not read profile data from an old sheet row, pinned node data, previous execution, global variable, or current UI selection.

If a user retries an old job, the default is the original snapshot. Running the same brief with updated company, direction, audience, or prompts creates a new snapshot, a new job, and a new approval.

### 6.6 SeoJob

Required fields:

- `job_id`
- `brief_id`
- `brief_fingerprint`
- `snapshot_id`
- `snapshot_hash`
- `company_id`
- `direction_id`
- `audience_segment_id`
- `state`
- `current_stage`
- `approved_plan_fingerprint`
- `approval_record_id`
- `attempt`
- `created_at`
- `started_at`
- `finished_at`
- `error_code`
- `error_summary`
- `artifact_manifest_path`

There is no global `current_company`, `current_direction`, or `current_manual` used by execution code.

### 6.7 ApprovalRecord

- `approval_record_id`
- `job_id`
- `approval_type`: `paid_execution`, `sheet_export`, or `final_publication`
- `snapshot_hash`
- `plan_fingerprint`
- `approved_by`
- `approved_at`
- `expires_at`

An approval is invalid when the snapshot, brief, company profile, direction, audience, prompt set, model plan, provider plan, or target destination changes.

## 7. Job State Machine

```text
DRAFT
  -> VALIDATED
  -> PLANNED
  -> AWAITING_PAID_APPROVAL
  -> QUEUED
  -> RUNNING
  -> SUCCEEDED
  -> AWAITING_EXPORT_APPROVAL
  -> EXPORTED
```

Failure transitions:

```text
QUEUED | RUNNING -> FAILED_RETRYABLE
FAILED_RETRYABLE -> QUEUED
QUEUED | RUNNING -> CANCELED
RUNNING -> FAILED_FINAL
```

Rules:

- only the worker mutates job state;
- every transition is atomic and append-audited;
- retries increment `attempt` and resume only from a checkpoint declared retry-safe;
- cancellation is cooperative and idempotent;
- a terminal job cannot return to a running state;
- export failure does not invalidate successful content generation.

## 8. Telegram Interaction Contract

### 8.1 Start

The user asks naturally or invokes the SEO flow. Gul’dan calls `seo_brief_start` and returns a draft identifier.

### 8.2 Company, Direction, and Audience Selection

The user first selects a company, then one of that company's business directions, then an audience segment belonging to that direction. Creating or replacing persistent client data is a separate confirmation.

The worker rejects impossible relationships even if a malformed tool call bypasses the Telegram wizard. A direction from another company and an audience from another direction can never be compiled into a snapshot.

Telegram displays a persistent context banner during brief creation:

```text
COMPANY: АвтоМаляр
DIRECTION: Кузовной ремонт и покраска
AUDIENCE: Владельцы автомобилей — Мариуполь
PROFILE VERSIONS: company=3, direction=5, audience=2
```

Changing any line invalidates the current plan and paid approval.

### 8.3 Brief Wizard

The wizard collects one logical field group at a time and persists after every accepted response. The user can request `покажи бриф`, `измени ключи`, `сменить направление`, `продолжить`, or `отменить` without losing accepted fields. Changing company clears the selected direction and audience. Changing direction clears the selected audience and any inherited page structure or category context.

### 8.4 Validation and Plan

Before approval, Telegram shows:

- normalized brief summary;
- company name and profile version;
- direction name and version;
- audience segment name and version;
- execution snapshot hash;
- competitor count;
- stages and planned providers/models;
- maximum retries;
- estimated paid-call envelope when pricing data is available;
- unknown costs explicitly marked unknown;
- destination and write behavior;
- warnings and blocked URLs.

### 8.5 Approval

The approval action includes both the snapshot hash and plan fingerprint. Plain conversational assent is accepted only when it unambiguously refers to the displayed company, direction, audience, and plan in the same session and both fingerprints are unchanged.

### 8.6 Progress

Fixed notifications contain:

- job ID;
- completed stage count;
- current stage;
- retry state;
- elapsed time;
- cancellation instruction.

Progress messages contain no scraped page content, prompts, credentials, or model reasoning.

### 8.7 Completion

Telegram receives:

- concise completion summary;
- Markdown artifact attachment;
- metadata attachment or compact preview;
- five titles;
- five descriptions;
- QA status;
- source count and provenance boundary;
- warnings;
- optional Sheets export action.

## 9. Stage B n8n Contract

### 9.1 Wrapper Responsibilities

The wrapper must:

- accept a signed JSON request from the worker only;
- reject missing, expired, duplicate, or invalid signatures;
- enforce idempotency by `job_id`, `brief_fingerprint`, and `snapshot_hash`;
- require the complete approved `ExecutionSnapshot` on every submission;
- verify that payload IDs, versions, and snapshot hash agree before execution;
- start the isolated workflow copy;
- expose status by job ID;
- return structured stage errors;
- return a structured result without writing Google Sheets;
- never expose a generic execute-any-workflow endpoint;
- never infer company, direction, audience, or page context from prior executions or n8n UI state.

### 9.2 Workflow Copy Changes

The isolated copy must:

1. replace manual trigger with a validated `ExecutionSnapshot` input;
2. replace hardcoded ResultUP data with explicit company, direction, audience, and brief fields from that snapshot;
3. remove source-sheet reads;
4. remove pinned profile/manual data and global fallback values;
5. make every profile-dependent expression resolve from the current job payload;
6. return structured output instead of writing the final sheet;
7. remove the disconnected RAG cluster and isolated Firecrawl nodes;
8. externalize prompts into versioned files or a prompt registry where n8n supports stable references;
9. emit stable stage identifiers and normalized errors;
10. preserve the source workflow untouched for differential comparison.

These are one-time parameterization changes to one isolated universal copy. They are not repeated when a company, direction, audience, or brief is created or edited. The processing topology remains stable until the pipeline logic itself is intentionally versioned.

Adding company-specific behavior uses validated data or an explicit `strategy_id` selected by the snapshot. It must not be implemented by cloning or rewriting nodes per company. A separate workflow version is justified only when the processing algorithm is materially different, not when brand, offer, audience, geography, tone, keywords, cases, or page structure differ.

### 9.3 Result Schema

The wrapper returns:

- `job_id`
- `workflow_execution_id`
- `status`
- `content_markdown`
- `titles[5]`
- `descriptions[5]`
- `keyword_qa`
- `text_metrics`
- `sources`
- `warnings`
- `model_usage`
- `stage_timings`
- `prompt_versions`

No credentials, full provider responses, chain-of-thought, or hidden reasoning may appear.

## 10. Stage C Migration Sequence

Migrate behind the same stage interfaces:

1. brief/profile validation and fingerprints;
2. URL normalization and SSRF policy;
3. deterministic n-gram extraction;
4. deterministic text metrics;
5. artifact manifest and provenance;
6. prompt registry and model routing;
7. scraper adapter and content normalization;
8. research adapter;
9. brief generation;
10. writer;
11. editor and link insertion;
12. keyword QA;
13. meta generation and evaluator;
14. optional Google Sheets export.

Each migrated stage is enabled by configuration for benchmark jobs only. Production cutover requires parity evidence and an explicit decision.

## 11. Security Controls

- Secrets remain outside config and artifacts, in worker-owned secret storage with `0600` files or an equivalent provider.
- The live Hermes plugin receives no provider credentials.
- n8n receives only the approved brief/profile payload.
- Wrapper requests use HMAC-SHA256, timestamp tolerance, nonce, and idempotency key.
- Callback payloads use the same authentication and are size-limited.
- User URLs resolve through an SSRF guard that blocks loopback, private, link-local, metadata, file, and non-HTTP schemes before and after redirects.
- Artifact paths and database queries are always scoped by `company_id`; direction- and audience-owned records additionally require matching `direction_id` and `audience_segment_id`.
- Logs redact secrets and truncate external payloads.
- Raw scraped pages have retention limits and are not sent to Telegram.
- Paid execution, Sheets export, and publication are separate approval records.
- The worker runs with bounded CPU, memory, concurrency, and request sizes.

## 12. Error Handling

Stable error classes:

- `BRIEF_INVALID`
- `PROFILE_INVALID`
- `URL_BLOCKED`
- `SCRAPE_FAILED`
- `RESEARCH_FAILED`
- `MODEL_RATE_LIMITED`
- `MODEL_OUTPUT_INVALID`
- `N8N_SUBMISSION_FAILED`
- `N8N_EXECUTION_FAILED`
- `ARTIFACT_INVALID`
- `EXPORT_FAILED`
- `CANCELED`

Retry policy:

- retry transient network, 429, 502, 503, and 504 failures with exponential backoff and jitter;
- do not retry validation, blocked URL, schema, or approval errors;
- cap retries per stage;
- preserve successful checkpoint outputs by content hash;
- surface retry exhaustion with the failed stage and safe next action.

## 13. Artifact Contract

Each successful job creates an immutable artifact directory containing:

- `content.md`
- `metadata.json`
- `qa.json`
- `sources.json`
- `manifest.json`

`manifest.json` records:

- job and profile versions;
- brief fingerprint;
- prompt versions;
- model/provider identifiers;
- source hashes and fetch timestamps;
- stage output hashes;
- warnings;
- timestamps;
- final status.

Artifacts never contain credentials or hidden model reasoning.

## 14. Verification Strategy

### 14.1 Contract Tests

- profile and brief schema validation;
- fingerprint stability;
- approval invalidation after changes;
- state transition guards;
- wrapper signing and replay rejection;
- idempotent duplicate submission;
- result schema validation;
- export approval boundary.

### 14.2 Security Tests

- SSRF bypass attempts using redirects, alternate IP forms, DNS rebinding-safe revalidation, IPv6, and encoded hosts;
- path traversal and symlink escape;
- cross-profile artifact access;
- secret redaction;
- oversized webhook payloads;
- stale signatures and nonce replay;
- canceled-job late callbacks.

### 14.3 Differential Benchmark

Use 3–5 fixed briefs that represent different page types and locales. Run the source workflow and candidate implementation from equivalent inputs. Compare:

- required section coverage;
- factual support and source traceability;
- keyword naturalness and overuse;
- internal-link correctness;
- language/locale compliance;
- title/description schema compliance;
- human editorial score;
- paid-call count;
- runtime;
- failure/retry behavior.

Exact text equality is not a success criterion.

## 15. Definition of Done — Stage B MVP

- Full brief can be created, resumed, reviewed, and validated in Telegram.
- Multiple companies and versioned direction/audience combinations can be selected without cross-company inheritance.
- A paid-call plan is shown and explicitly approved.
- One signed, idempotent job executes through the isolated n8n copy.
- Progress reaches Telegram without a second polling bot and without an LLM call for fixed messages.
- Gateway restart does not lose job state.
- Worker restart resumes or safely fails a job from durable state.
- Cancel and retry are idempotent.
- Markdown, metadata, QA, sources, and manifest artifacts validate against schemas.
- Google Sheets remains unchanged until a separate export approval.
- The original workflow remains unchanged.
- Creating, editing, selecting, or archiving a company causes zero n8n node mutations.
- Two jobs for different companies can run sequentially through the same universal workflow copy and produce manifests with different snapshot hashes and no cross-company fields.
- Security, contract, and end-to-end tests pass.
- One benchmark brief completes from Telegram to local artifacts and an approved optional Sheets export.

## 16. Approval Boundaries for Implementation

Local specification, isolated repository scaffolding, tests, mock servers, and non-network prototypes are safe to perform autonomously after plan approval.

Separate explicit approval is required before:

- creating or editing the n8n wrapper or workflow copy;
- extending n8n OAuth scopes;
- creating n8n webhooks;
- using paid provider calls;
- writing Google Sheets;
- sending progress or artifacts through live Telegram as part of testing;
- installing or enabling a Hermes plugin in the live profile;
- adding systemd services or changing gateway configuration;
- deploying the worker;
- publishing generated content.

## 17. Implementation Planning Boundary

The implementation plan must split work into independently testable tracer bullets:

1. local domain contracts and state machine;
2. profile and brief persistence;
3. local worker API and mock execution adapter;
4. Hermes plugin against the mock worker;
5. notification webhook adapter in an isolated test profile;
6. n8n wrapper contract and mock server;
7. approved n8n integration;
8. artifact and optional Sheets export;
9. differential benchmark;
10. staged migration from n8n to native worker stages.

No production or external integration step may be folded into local scaffolding tasks.