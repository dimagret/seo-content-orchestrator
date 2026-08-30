# Codex Responses lifecycle evidence — read-only review

## Verdict

```text
NOT PROVEN / FAIL-CLOSED
```

The public OpenAI Responses API documents background response retrieval and idempotent cancellation. That is evidence for `https://api.openai.com/v1`, not for the consumer OAuth transport that Hermes uses for the active `openai-codex` provider. Read-only Hermes source evidence shows a materially different transport: `https://chatgpt.com/backend-api/codex`, invoked through streaming `responses.create(stream=True)` with `store=False`.

No primary-source evidence presently proves that this consumer transport supports durable request idempotency, retrieve-after-crash, or cancel for a broker-owned run. It is therefore unsafe to implement or claim a durable automated provider path on it.

Inspected Hermes build: `71e7eb3c168a49fdd3179efb8a921ad78f6e8e1d` (`/opt/hermes/.hermes_build_sha:1`).

## Evidence table

| Requirement | Evidence | Assessment |
|---|---|---|
| Public Responses background lifecycle | OpenAI Background mode guide documents `background: true`, polling `GET /v1/responses/{id}`, and `POST /v1/responses/{id}/cancel`. It states repeated cancellation returns the final response. Source: https://developers.openai.com/api/docs/guides/background/ | **Supported only for public API** |
| Public request idempotency | The inspected public Background mode guide contains no `Idempotency-Key` contract. A response ID is available only after create succeeds. It also says a background response with `store` omitted or false is deleted after roughly 10 minutes. | **Not proven** |
| Hermes active provider endpoint | Hermes `openai-codex` profile sets `base_url="https://chatgpt.com/backend-api/codex"` and `api_mode="codex_responses"`. Source: `/opt/hermes/plugins/model-providers/openai-codex/__init__.py:6-13`. | **Supported** |
| Hermes request behavior | The Codex transport builds `store=False` and calls `active_client.responses.create(stream=True)`. The adapter normalizes/rejects non-false `store`, and its typed allowlist has no `background` field. Sources: `/opt/hermes/agent/transports/codex.py:301-312`; `/opt/hermes/agent/codex_responses_adapter.py:908-924`, `912-918`, `1022-1026`; `/opt/hermes/agent/codex_runtime.py:1228-1268`. | **Supported** |
| Consumer response ID | Hermes extracts `response.id` only from a terminal stream event. Source: `/opt/hermes/agent/codex_runtime.py:973-1004`, `1215-1224`. A crash before that terminal event leaves no demonstrated broker-retrievable ID. | **Insufficient for crash recovery** |
| Consumer timeout behavior | Hermes retries a stream connect or mid-stream transport failure once. Its normal request headers include `session_id` and `x-client-request-id` for cache scope, not a documented idempotency key. Sources: `/opt/hermes/agent/codex_runtime.py:1259-1276`, `1323-1332`; `/opt/hermes/agent/transports/codex.py:404-426`. This is evidence that the existing path does not establish durable upstream acceptance before retrying. | **Incompatible with a no-duplicate canary without a new proof** |
| Existing Hermes API server idempotency | `Idempotency-Key` is supported at the local API server, but `_IdempotencyCache` is explicitly in-memory, TTL 300 seconds. Source: `/opt/hermes/gateway/platforms/api_server.py:1033-1078`. | **Not crash-durable** |
| Existing Hermes API server response lookup | Stored Responses can be retrieved by ID, but that store is downstream of agent completion and does not prove upstream consumer-Codex acceptance/retrieval. Source: `/opt/hermes/gateway/platforms/api_server.py:5100-5147`. | **Not enough** |

## What this rules out

- Running a real SEO provider canary through the current `hermes proxy` or local API server while claiming durable external-job semantics.
- Reusing the runtime's `x-client-request-id` / session ID as an idempotency proof; the inspected source uses it for cache scope, not a documented durable run lookup.
- Retrying an unknown consumer-Codex request automatically after a client/proxy crash.

## What would change the verdict

Only primary evidence for the actual `chatgpt.com/backend-api/codex` OAuth transport can open the gate:

1. an official contract that binds a client-generated idempotency key to one durable response/run;
2. a documented authenticated retrieval operation for that run after process crash;
3. a documented cancellation operation with terminal-state semantics;
4. an isolated, explicitly approved non-production probe that verifies all three without exposing credentials or producing publishable content.

Until then, a broker may record `RECONCILIATION_REQUIRED` on uncertain acceptance, but it cannot safely resubmit or resolve the job. The correct automated-pipeline verdict remains fail-closed.

---

## Local provenance

- Source path: `/opt/data/hermes-openai-codex-broker/docs/codex-lifecycle-evidence.md`
- Source body SHA-256: `b7c2b99e3c724fd1ffb413024b3e1a475f12d8e975fadc7fee5c51911242c5a4`
- Copy rule: the first `4774` bytes are the source body copied unchanged; their verified SHA-256 is the value above.
- Source Git commit: unavailable (`git rev-parse --verify HEAD` returned no revision); no commit-pinned provenance is claimed.
- Scope: read-only lifecycle evidence; it does not authorize provider execution.
