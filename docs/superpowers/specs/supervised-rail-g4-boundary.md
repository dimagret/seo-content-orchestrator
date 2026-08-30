# Supervised subscription rail — G4 boundary

Этот документ фиксирует, что **дизайн, план и evidence не авторизуют** реальный model stage. Отдельный G4 approval нужен до первого использования подписки.

## Что НЕ авторизует текущий design

- вызовов `gpt-5.6-terra via openai-codex`;
- импорта completion из Hermes;
- публикации, Telegram, Sheets, n8n, deployment;
- commit/push/PR без отдельного решения;
- модификаций `/opt/hermes/*`;
- модификаций design-only broker project (`/opt/data/hermes-openai-codex-broker`) для запуска.

## Что авторизует текущий design

- локальные команды supervised-packet / supervised-bind / supervised-status без сетевых вызовов;
- локальные pytest/ruff/mypy в worktree;
- чтение design / plan / evidence / operator playbook.

## Что должен утвердить владелец перед G4 canary

До первого реального model stage оператор/владелец должен явно подтвердить каждый пункт. Без полного списка `PASS` — canary **запрещён**.

### 1. Frozen brief

```text
company_id, brief_id, snapshot_id, snapshot_hash,
approved_plan_fingerprint, approval_record_id
```

Эти значения должны быть зафиксированы в supervised-packet output до шага 1.

### 2. URL allowlist

Точный список доменов, которые будут использованы для evidence acquisition. Любой запрос вне allowlist должен быть отклонён supervised rail на стадии prepare_packet.

### 3. Четыре пакета

```text
stage sequence: outline → draft → critic → revision
provider_id:    openai-codex
model_id:       gpt-5.6-terra
prompt_template_version: supervised-subscription-v1
```

Не должно быть `supervised-recovery`, `supervised-cancel`, `supervised-complete`, `supervised-execute`, `provider`, `hermes` (это проверено в `test_supervised_cli_allowlist_contains_no_execution_command`).

### 4. Token/usage ceiling

Максимальный суммарный объём использования подписки на один canary run. Это **не** контролируется supervised rail — это **owner-attested gate**.

### 5. Видимая сессия

`--session-ref` одной конкретной Hermes-сессии. После canary эта сессия не используется повторно для supervised rail до явного нового решения.

### 6. Локальная точка сохранения

Где именно будут созданы `completion-file` и где `seo-orchestrator` их прочтёт. Это **owner-attested gate** для файловой системы.

### 7. G4 approval artifact

Один файл (или серия файлов) в worktree, где зафиксированы пункты 1–6 и явное `APPROVED G4 canary` от владельца. Без этого файла canary запрещён.

## Что оператор видит до утверждения

До G4 approval оператор может видеть только:

- вывод `seo-orchestrator supervised-packet` (canonical JSON, без сетевых вызовов);
- вывод `seo-orchestrator supervised-status` (canonical JSON, без сетевых вызовов);
- `git status --short` и `git log` в worktree;
- локальный pytest/ruff/mypy отчёт;
- read-only документацию: design / plan / evidence / operator playbook / failure states / этот документ.

Оператор **не** должен видеть:

- HTTP-запросы к `chatgpt.com` или `api.openai.com`;
- фоновые процессы Hermes proxy;
- OAuth / credentials;
- сетевые публикации.

Если что-то из последнего списка появляется — canary запрещён до отдельного owner-decision gate.

## Граница для финального artifact

`ARTIFACT_FROZEN` означает:

- immutable `content.md`, `metadata.json`, `qa.json`, `sources.json`, `manifest.json`;
- 0o440 режим, owner-only, single-link;
- verified manifest path в supervised rail ledger;
- JobService state `SUCCEEDED` с привязанным manifest path.

До `ARTIFACT_FROZEN` материал не считается завершённым. После `ARTIFACT_FROZEN` материал ожидает manual review и **отдельного решения** о любом дальнейшем шаге.