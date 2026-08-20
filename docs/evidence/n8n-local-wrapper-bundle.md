# Task 17 local-only n8n wrapper bundle evidence

Status: `LOCAL_MOCK_ONLY_NOT_CLOUD_VERIFIED`

This document records the local artifact prepared after the user explicitly chose to continue without n8n Cloud mutation. It is not evidence of Cloud import, activation, concurrency safety, or provider execution.

## Scope

- No n8n Cloud API, UI, MCP, workflow, credential, or Data Table was mutated.
- No provider execution or paid canary was performed.
- No credential values from the supplied archive were read into this repository.
- The supplied raw ZIP remains outside version-controlled artifacts.

## Source provenance

The reproducible provenance boundary starts at the committed, sanitized Task 16 files `source-workflow.json`, `expected-node-map.json`, and `universal-contract.json`. The generated universal workflow must equal `transform_workflow(source-workflow.json)` byte-for-byte and pass the existing universal validator.

The user-supplied backup archive and raw workflow exports are deliberately outside the candidate and are not acceptance evidence. Session-only inspection results are not claimed as independently reproducible repository provenance.

## Reproducible artifact

Artifact: `integrations/n8n/stage-b-local-bundle.json`

SHA-256:

`669df2d880e23429f98ffa03cf7b008829bdc0266a2368b103f2b7fdacd6bc2c`

Regenerate or verify:

```bash
uv run python -m integrations.n8n.build_wrapper_bundle
uv run python -m integrations.n8n.build_wrapper_bundle --check
```

The bundle contains:

- the byte-exact validated Task 16 universal workflow artifact;
- an inactive mock-only wrapper workflow blueprint;
- four frozen routes: submit, lookup, poll, and cancel;
- Webhook raw-body capture followed by binary SHA-256;
- timestamp, nonce, idempotency-header, signature-format, and skew preflight;
- strict submit/lookup/status/cancel body validation;
- snapshot and brief fingerprint recomputation, authority-expiry enforcement, and bound model/provider authorization;
- HMAC-SHA256 through an unresolved n8n Crypto credential placeholder;
- constant-time signature comparison;
- per-route nonce lookup, replay rejection, and nonce insertion;
- semantic submit idempotency that preserves the first external run identity and returns `409` on a changed request hash;
- lookup `200`/`404` behavior; poll/cancel first select by the HMAC-signed external run path and then reject an idempotency header whose hash differs from the durable row;
- a hard-false G4 provider gate in front of the bound universal sub-workflow execution path;
- a privacy-bounded `seo_wrapper_requests` Data Table schema;
- unresolved Cloud bindings and verification gates.

## Fail-closed gates

The local validator reports the artifact as statically valid but not deployable while any of these remain unresolved:

- `cloud_import_verified`;
- `data_table_atomic_uniqueness_verified`;
- `free_mock_probe_completed`;
- `crypto_hmac_credential_id`;
- `data_table_id`;
- `universal_workflow_id`;
- `approved_model_ids`;
- `approved_provider_ids`.

The wrapper and universal workflow are inactive. The provider gate is a literal `false`, so paid/provider execution is unreachable in the local artifact. Replacing placeholders, changing the provider gate, enabling workflows, changing the table schema, disabling raw-body capture, or drifting the frozen universal graph invalidates the local bundle.

## Known boundary

n8n Data Table uniqueness and concurrent duplicate-submit behavior cannot be proven statically. Sequential nonce rejection and semantic duplicate handling are represented in the graph, but the blueprint must not be activated until a controlled Cloud import and free mock-only replay/concurrency probe establish atomic uniqueness. Paid/provider execution remains unreachable and requires separate G4 approval plus an explicit gate change.
