# Supervised subscription rail — documentation index

Этот файл связывает дизайн, план реализации, lifecycle-evidence и код supervised rail в одном worktree. Никаких новых поведенческих или внешних действий — только markdown.

## Документы

| Документ | Назначение |
| --- | --- |
| [`./2026-08-23-supervised-subscription-rail-design.md`](./2026-08-23-supervised-subscription-rail-design.md) | Standard design: почему `gpt-5.6-terra via openai-codex` используется только внутри видимой Hermes-сессии; что входит и что не входит в контур. |
| [`./2026-08-23-codex-lifecycle-evidence.md`](./2026-08-23-codex-lifecycle-evidence.md) | Read-only lifecycle evidence: verdict `NOT PROVEN / FAIL-CLOSED` для unattended durable provider executor поверх consumer `chatgpt.com/backend-api/codex`. |
| [`../plans/2026-08-23-supervised-subscription-rail.md`](../plans/2026-08-23-supervised-subscription-rail.md) | Implementation plan с пятью задачами (TDD: red → green → refactor), acceptance, file structure. |
| [`./supervised-rail-operator-playbook.md`](./supervised-rail-operator-playbook.md) | Operator playbook: точные команды и требования к видимой Hermes-сессии; ни одного шага, который звонит в provider. |
| [`./supervised-rail-failure-states.md`](./supervised-rail-failure-states.md) | Failure states: что делает оператор при `OPERATOR_RECOVERY_REQUIRED` и `CANCEL_REQUESTED`; какие действия запрещены. |
| [`./supervised-rail-g4-boundary.md`](./supervised-rail-g4-boundary.md) | G4 boundary: что именно должен утвердить владелец перед первым реальным model stage; какие артефакты показать оператору до approval. |

## Реализация (ссылка на код)

| Файл | Назначение |
| --- | --- |
| `src/seo_orchestrator/supervised_rail.py` | StagePacket, ObservedCompletion, private SQLite ledger, prepare_packet / bind_completion / request_cancel / mark_operator_recovery_required. |
| `src/seo_orchestrator/services/supervised_subscription.py` | SupervisedSubscriptionFinalizer; связывает rail с `JobService` и `ArtifactStore` без provider/Hermes/OAuth. |
| `src/seo_orchestrator/cli.py` (supervised-packet / supervised-bind / supervised-status) | Offline CLI без сетевых вызовов. |
| `tests/contract/test_supervised_rail.py` | Contract tests: hash determinism, attestation binding, recovery, cancellation. |
| `tests/integration/test_supervised_subscription.py` | Integration tests: четыре оператор-attested stage создают один локальный immutable artifact. |
| `tests/unit/test_runner_cli.py` | CLI guard-rails: только allowlisted команды, запрет execution-флагов. |

## Acceptance gates (read-only, без model call)

Эти gates выполняются **локально**, до любого subscription use:

```text
- contract supervised rail:        51 passed
- integration supervised rail:     24 passed
- CLI unit tests:                   9 passed
- pytest tests:                  1726 passed
- ruff check src tests:          PASS
- mypy src:                     PASS (42 source files)
- git diff --check:              PASS
- provider/model calls:               0
- Hermes proxy launches:              0
- OAuth/credential/config changes:    0
- Telegram, Sheets, n8n, publication: 0
```

Эти gates **не** означают G4. Они подтверждают только то, что локальный supervised rail работает без внешних эффектов.

## Что НЕ входит в этот дизайн

- unattended durable provider executor (заблокирован lifecycle evidence);
- local durable proxy поверх consumer `chatgpt.com/backend-api/codex`;
- любые network acquisition на шагах supervised rail;
- извлечение OAuth / API key / credentials;
- публикация, Telegram, Sheets, n8n, deployment.

Любая попытка добавить эти действия должна идти через отдельный G-уровень и отдельный owner-decision gate.