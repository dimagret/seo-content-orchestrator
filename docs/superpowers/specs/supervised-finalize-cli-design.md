# `supervised-finalize` CLI — design summary

## Goal

Close the operational gap after the fourth supervised bind with one explicit,
local-only command that invokes the existing
`SupervisedSubscriptionFinalizer.finalize()` contract.

## Proposed command

```bash
seo-orchestrator supervised-finalize \
  --company-id avtomalyar-real-context \
  --job-id job-a840a703682a4a6e9ec43a7297302952
```

## Behaviour

1. Load `Settings` and derive the authoritative supervised ledger path from
   `settings.db_path`.
2. Open the authoritative SQLite connection and construct the scoped
   `JobService`.
3. Construct `ArtifactStore` from the configured artifact root.
4. Construct `SupervisedRail` and `SupervisedSubscriptionFinalizer`.
5. Call `finalize(job_id)` exactly once.
6. Emit canonical JSON containing `status=ARTIFACT_FROZEN` and the returned
   manifest identity.

The command is fail-closed: any missing job, wrong rail state, binding mismatch,
non-authoritative path, invalid revision, or artifact error exits non-zero and
must not claim success.

## Security boundary

The command performs no HTTP, provider, Hermes proxy, OAuth, credential,
Telegram, Sheets, n8n, publication, or deployment action. It is an explicit
local operator command, not a worker executor and not an automatic retry path.

## Tests first

- CLI parser exposes `supervised-finalize` with only `--company-id` and
  `--job-id`.
- A real integration flow reaches `FINAL_QA_READY`, invokes the CLI entrypoint,
  and verifies `ARTIFACT_FROZEN`, immutable artifact files, and JobService
  `SUCCEEDED`.
- Repeating finalize is idempotent and returns the same manifest identity.
- Finalize from a non-terminal rail state fails closed.
- CLI allowlist remains free of provider/network execution verbs.

## Playbook correction

After the CLI is implemented, update the operator playbook to:

- use current branch/commit-agnostic prerequisites;
- use `/opt/data/cache/t23-canary-2026-08-23/completions/`, never `/tmp`;
- document `supervised-finalize` as the final local step;
- distinguish `FINAL_QA_READY` from `ARTIFACT_FROZEN` and manual review.

## Scope

In scope: `cli.py`, focused CLI/integration tests, and the operator playbook.

Out of scope: provider transport, Hermes configuration, OAuth, deployment,
publication, JobService state-machine redesign, and any new retry semantics.
