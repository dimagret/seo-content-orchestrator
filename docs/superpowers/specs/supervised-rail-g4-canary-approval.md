# G4 canary approval artifact — «АвтоМаляр» supervised rail

> Это approval-артефакт для первого реального supervised-stage run. Frozen inputs получены **read-only** из авторитативной БД `worker.db` (Task 21 mock-прогон, `job-a840a703682a4a6e9ec43a7297302952`). Owner-decision поля заполнены default значениями; **owner-decision recorded 2026-08-30 (`dimagret`)**: точная фраза `APPROVED G4 canary` проставлена в разделе 11 и применяется ровно к одному operator-attested canary run со значениями разделов 1–6.

## 0. Approval block

```text
G4 canary:                          APPROVED G4 canary — owner-attested for one canary run (2026-08-30, dimagret)
Company:                             АвтоМаляр / avtomalyar-real-context
Job:                                  job-a840a703682a4a6e9ec43a7297302952
Snapshot:                             snapshot-393359ff2ffc4f59b5c274ac45045c07
Snapshot hash:                        f36d29e66ed09d3b71461f9b503c25bdf3cf684ded885d564b761a8f976a408e
Approved plan fingerprint:            6686b374a7ff0f34fa938e23d15bc45ec3535e0f42eb4b473c1a95757038e134
Approval record id:                   approval-ed3cab7e145c4d789565f4cbc01f45b4
Provider:                             openai-codex
Model:                                gpt-5.6-terra
Pipeline version:                     supervised-subscription-v1
Maximum retries:                      0
Stages:                               outline → draft → critic → revision (4 stages, fixed)
Result destination:                   local-artifacts (no Telegram/Sheets/n8n/publication)
Created by:                           local-real-context-operator
Created at:                           2026-08-23T16:06:59.329726+00:00
Session ref (default):                hermes-session-7f3a9c2e
Operator id (default):                dimagret-canary-2026-08-23
Operator role (default):              local-owner-supervised-pilot
```

Этот блок копируется оператором в supervised-packet output при первом запуске `seo-orchestrator supervised-packet`. Совпадение значений обязательно для binding.

## 1. Frozen brief

```text
company_id:                  avtomalyar-real-context
brief_id:                    brief-e7a8f02075f14ca5a2cd2a80ccb601ce
brief_fingerprint:           af8135e771c9ff5914a81b405401a88e5c79c59ad99c25b3da7d31e96190050b
snapshot_id:                 snapshot-393359ff2ffc4f59b5c274ac45045c07
snapshot_hash:               f36d29e66ed09d3b71461f9b503c25bdf3cf684ded885d564b761a8f976a408e
company_profile_version:     1
direction_id:                body-repair
direction_version:           1
audience_segment_id:         car-owners-mariupol
audience_version:            1
prompt_set_version:          1
approved_plan_fingerprint:   6686b374a7ff0f34fa938e23d15bc45ec3535e0f42eb4b473c1a95757038e134
approval_record_id:          approval-ed3cab7e145c4d789565f4cbc01f45b4
```

Brief scope:

```text
primary_keyword:             кузовной ремонт автомобилей в Мариуполе
locale:                      ru-RU
target_language:             ru
page_type:                   service-page
competitor_urls:              https://kotlyar-avto.ru/
                              https://stosfera.ru/
                              https://sto-mariupol.ru/
current_page_url:            https://www.avtomalyarmrpl.ru/
```

## 2. URL allowlist (frozen evidence acquisition)

Только эти домены могут появляться в `evidence.sources[*].url`. Любой другой URL должен быть отклонён `validate_source_provenance` / `validate_artifact_safe_value`.

```text
https://www.avtomalyarmrpl.ru/
https://kotlyar-avto.ru/
https://stosfera.ru/
https://sto-mariupol.ru/
```

Это allowlist **доменов**, не allowlist поведения. Любые автоматические fetches остаются **запрещены**: фактические `sources` в supervised rail создаются вручную оператором и помещаются в frozen evidence через completion-file.

## 3. Четыре пакета

```text
sequence:            outline → draft → critic → revision
provider_id:         openai-codex
model_id:            gpt-5.6-terra
prompt_template_version: supervised-subscription-v1
attestation fields:  session_ref, operator_id, provider_id, model_id, observed_at
maximum_retries:     0
```

Проверки, которые не должны срабатывать ни на одном пакете:

```text
- supervised-recovery, supervised-cancel, supervised-complete, supervised-execute
- --state-path / --alternate-sqlite
- provider, hermes
```

Это покрыто `test_supervised_cli_allowlist_contains_no_execution_command` и `test_supervised_modules_import_no_transport_or_process_clients`.

## 4. Token / usage ceiling

Supervised rail **не контролирует** расход подписки. Owner-attested gate для canary run:

```text
max_stages:                     4
max_session_minutes:            30
expected_subscription_use:      one Hermes session for 4 stages
expected_token_ceiling:         120000
expected_dollar_ceiling:        1.00
```

Эти значения **не** попадают в supervised rail. Они фиксируются на бумаге и проверяются после canary по истории Hermes-сессии.

## 5. Видимая сессия

```text
session_ref:                    hermes-session-7f3a9c2e
single_use:                     yes
reuse_for_other_jobs:           no
operator_id:                    dimagret-canary-2026-08-23
operator_role:                  local-owner-supervised-pilot
```

После canary эта сессия не используется повторно для supervised rail до отдельного owner-decision.

## 6. Локальная точка сохранения

Где оператор создаёт и где `seo-orchestrator` читает completion-file. Owner-attested gate для файловой системы.

```text
completion_root:                /opt/data/cache/t23-canary-2026-08-23/completions/
mode:                           0o600
single_link:                    yes
owner_uid_match:                required
no_follow:                      required (O_NOFOLLOW)
size_limit:                     MAX_CANONICAL_BYTES (one canonical JSON envelope)
naming:                         completion-<stage_id>-<sequence>.json
```

Прежде чем `seo-orchestrator supervised-bind` прочтёт файлы, оператор создаёт каталог:

```bash
install -d -m 0700 /opt/data/cache/t23-canary-2026-08-23/completions/
```

Эти требования уже реализованы в `_read_private_json` (см. `cli.py`).

## 7. Evidence, plan, design cross-references

```text
design:                         docs/superpowers/specs/2026-08-23-supervised-subscription-rail-design.md
plan:                           docs/superpowers/plans/2026-08-23-supervised-subscription-rail.md
operator playbook:              docs/superpowers/specs/supervised-rail-operator-playbook.md
failure states:                 docs/superpowers/specs/supervised-rail-failure-states.md
G4 boundary:                    docs/superpowers/specs/supervised-rail-g4-boundary.md
lifecycle evidence:             docs/superpowers/specs/2026-08-23-codex-lifecycle-evidence.md
documentation index:            docs/superpowers/specs/supervised-rail-index.md
```

## 8. Local verification status (read-only)

```text
pytest tests/contract/test_supervised_rail.py     51 passed
pytest tests/integration/test_supervised_subscription.py   24 passed
pytest tests/unit/test_runner_cli.py               9 passed
pytest tests                                     1726 passed
ruff check src tests                              PASS
mypy src                                          PASS (42 source files)
git diff --check                                  PASS
provider/model calls                              0
Hermes proxy launches                             0
OAuth/credential/config changes                   0
Telegram, Sheets, n8n, publication                0
```

## 9. Definition of done for canary

Canary считается успешным, **только если** выполнены все пункты:

1. `seo-orchestrator supervised-packet` для `outline → draft → critic → revision` отработал без сетевых вызовов, вернул 4 канонических packet envelope.
2. Оператор в видимой Hermes-сессии для каждого пакета получил bounded completion envelope с `company_id/job_id/stage_id/input_hash/payload`, скопированными из packet envelope.
3. `seo-orchestrator supervised-bind` принял все 4 completion-file; каждый прошёл private-file check (`0o600`, single-link, owner match, `O_NOFOLLOW`).
4. После 4-го bind в `supervised-status` появился `FINAL_QA_READY`.
5. Локальный finalizer (`SupervisedSubscriptionFinalizer.finalize`) записал immutable `content.md`, `metadata.json`, `qa.json`, `sources.json`, `manifest.json`; `JobService` переведён `RUNNING → SUCCEEDED`; manifest path привязан к job.
6. Token/dollar ceiling из пункта 4 не превышен.
7. После canary эта `session_ref` помечена «использована» и не используется повторно до отдельного решения.
8. Manual review оператором подтвердил качество материала без публикации.

## 10. Что НЕ будет делать canary

- не отправит ничего в Telegram / Google Sheets / n8n;
- не выполнит deployment;
- не будет читать или извлекать OAuth / credentials / API keys;
- не будет пытаться отменить upstream запрос через `chatgpt.com/backend-api/codex`;
- не будет делать скрытый retry, если стадия не подтверждена;
- не будет автоматически продолжать работу после `OPERATOR_RECOVERY_REQUIRED` или `CANCEL_REQUESTED`;
- не будет передавать `ARTIFACT_FROZEN` материал ни в какую внешнюю систему без отдельного решения.

## 11. Owner-decision gate

```text
APPROVED G4 canary — финальная фраза владельца (точно):

APPROVED G4 canary                 ← exact owner-attested phrase, single-use, 2026-08-30

Подпись: dimagret
Дата:    2026-08-30
```

`APPROVED G4 canary` — это **точная фраза владельца**, зафиксированная в публичном commit-е. Без неё canary был запрещён. Этот commit применяет её ровно к одному operator-attested canary run со значениями, перечисленными в разделах 1–6. После canary любая повторная попытка требует свежей фразы и нового owner-decision.

---

## Local provenance

- Frozen inputs получены read-only SQL из `/opt/data/cache/t21-real-context-run/worker.db`. Это та же БД, на которой Task 21 выполнил production-like local mock прогон. Никаких изменений не делалось.
- Approval artifact записан в worktree `feat/task-23-supervised-subscription-rail`, без commit и push.
- Default values зафиксированы 2026-08-30 оператором (`dimagret-canary-2026-08-23`) как starter set; их можно переопределить до фактического `APPROVED G4 canary`.
- Любая правка frozen brief или allowlist требует нового owner-decision gate.

## Boundary re-statement

After the owner-decision commit:

- `seo-orchestrator supervised-packet` and `seo-orchestrator supervised-bind` are authorised to be invoked by the operator (`dimagret-canary-2026-08-23`) against `avtomalyar-real-context / job-a840a703682a4a6e9ec43a7297302952`;
- `SupervisedSubscriptionFinalizer.finalize` is authorised to write the immutable artifact under the configured `artifact_root` and transition `JobService` `RUNNING → SUCCEEDED`;
- no publication, Telegram, Sheets, n8n, or deployment is authorised;
- no extraction of OAuth, credentials, or API keys is authorised;
- no unattended durable provider executor is authorised.

If the operator runs more than the documented four stages (`outline → draft → critic → revision`) or exceeds `expected_token_ceiling` / `expected_dollar_ceiling`, this approval is exhausted and a new gate is required.