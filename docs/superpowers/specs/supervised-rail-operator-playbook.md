# Supervised subscription rail — operator playbook

Этот playbook описывает **как оператор** в видимой Hermes-сессии безопасно использует supervised rail. Никакие команды ниже не вызывают provider напрямую — оператор сам работает в подписке и вручную передаёт результат через локальный completion-file.

## Предусловия

```text
- в worktree находится проверенный актуальный commit supervised rail;
- локальный pytest/ruff/mypy зелёные;
- Hermes CLI готов к работе (у оператора);
- operator_id известен (например, "operator-auto");
- session_ref известен — идентификатор видимой Hermes-сессии.
```

## Шаг 1. Подготовка канонического пакета

```bash
seo-orchestrator \
  supervised-packet \
  --company-id avtomalyar-real-context \
  --job-id <JOB_ID> \
  --session-ref <VISIBLE_HERMES_SESSION_REF>
```

Команда **только** возвращает canonical JSON со stage `outline`. Никаких сетевых вызовов.

Ожидаемый вывод содержит:

```text
job_id, company_id, stage_id="outline", sequence,
approval_record_id, approved_plan_fingerprint,
snapshot_hash, evidence_hash,
provider_id="openai-codex", model_id="gpt-5.6-terra",
designated_session_ref,
prompt_template_version="supervised-subscription-v1",
previous_completion_hash,
input_hash,
prompt
```

`input_hash` нужно сохранить — он используется в шаге 3 для binding.

## Шаг 2. Выполнение stage в видимой Hermes-сессии

Оператор копирует `prompt` целиком и просит Hermes выполнить его **внутри видимой сессии**. Ожидаемая форма ответа — bounded JSON envelope:

```json
{
  "company_id": "avtomalyar-real-context",
  "job_id": "<JOB_ID>",
  "stage_id": "outline",
  "input_hash": "<input_hash from step 1>",
  "payload": { /* stage-specific structured data */ }
}
```

Правила:

- запрещены prose вокруг JSON, markdown fences, скрытое рассуждение;
- `company_id`, `job_id`, `stage_id`, `input_hash` копируются **из пакета** — модель их не выдумывает;
- `payload` соответствует stage schema.

## Шаг 3. Сохранение completion-file

Оператор сохраняет bounded envelope в файл `0o600`, single-link, в каталоге, доступном только ему.

```bash
install -d -m 0700 /opt/data/cache/t23-canary-2026-08-23/completions/
install -m 0600 /dev/null /opt/data/cache/t23-canary-2026-08-23/completions/completion-outline-1.json
# записать bounded envelope в файл (например через tee)
```

## Шаг 4. Локальный binding

```bash
seo-orchestrator \
  supervised-bind \
  --company-id avtomalyar-real-context \
  --job-id <JOB_ID> \
  --completion-file /opt/data/cache/t23-canary-2026-08-23/completions/completion-outline-1.json \
  --operator-id operator-auto \
  --session-ref <VISIBLE_HERMES_SESSION_REF> \
  --provider-id openai-codex \
  --model-id gpt-5.6-terra
```

Команда:

- читает completion-file через `_read_private_json`: абсолютный нормализованный путь, `O_NOFOLLOW`, режим `0o600`, `nlink=1`, owner == текущий uid;
- проверяет, что `company_id`/`job_id` совпадают с запрошенными;
- проверяет `input_hash` против outstanding packet;
- сохраняет immutable completion в private ledger;
- возвращает **следующий** пакет (`draft`) или terminal status.

Если что-то не совпало — оператор получает `ValueError` без записи в ledger.

## Шаг 5. Повторить шаги 1–4 для `draft`, `critic`, `revision`

Последовательность фиксирована. После четвёртого bind команда возвращает статус `FINAL_QA_READY`.

## Шаг 6. Finalize

Этот шаг выполняется оператором явной локальной CLI-командой. Команда **не** отправляет данные во внешние системы:

```bash
seo-orchestrator \
  supervised-finalize \
  --company-id avtomalyar-real-context \
  --job-id <JOB_ID>
```

Внутри вызывается существующая функция `SupervisedSubscriptionFinalizer.finalize(job_id)`:

1. проверяет, что rail в `FINAL_QA_READY`;
2. делает `JobService.transition RUNNING → SUCCEEDED` (через scoped JobService);
3. пишет immutable artifact в `ArtifactStore`;
4. привязывает manifest path/hash к job;
5. записывает verified manifest path в ledger как `ARTIFACT_FROZEN`.

После этого оператор видит в `supervised-status` запись `ARTIFACT_FROZEN` с `manifest_path` и `manifest_hash`.

## Шаг 7. Manual review

Manual review остаётся за оператором. До завершения manual review ни одна внешняя система (Telegram, Sheets, n8n, публикация, деплой) не получает никакого сигнала.

## Что оператор делать НЕ должен

- загружать completion-file из общего каталога или по относительному пути;
- пытаться "пропустить" стадию или изменить `stage_id`;
- править `input_hash` или `provider_id/model_id` в completion-file после наблюдения сессии;
- запускать supervised-bind без `--session-ref` — без привязки к видимой сессии binding отклоняется;
- пытаться выполнить supervised-bind, когда rail в `OPERATOR_RECOVERY_REQUIRED` или `CANCEL_REQUESTED` без явного решения по recovery/cancel (см. [`supervised-rail-failure-states.md`](./supervised-rail-failure-states.md));
- записывать completion-file из другой сущности, кроме той Hermes-сессии, которую он указал в `--session-ref`.

## Что произойдёт при попытке нарушить правила

| Нарушение | Поведение |
| --- | --- |
| completion-file с mode != `0o600` | `ValueError: completion file is not a private bounded regular file` |
| completion-file как symlink | `ValueError: completion file is not a private bounded regular file` |
| дублирование ключей в JSON | `ValueError: completion file is not valid bounded JSON` |
| `company_id/job_id` mismatch | `ValueError: completion file is outside the requested authority scope` |
| `input_hash` mismatch | `ValueError: ... does not match outstanding packet` |
| неподдерживаемый `stage_id` | `ValueError: stage_id is not supported` |
| не-frozen evidence citations | `ValueError: content citation set must exactly reference frozen sources` |
| credential-like text в `content_markdown` | `ValueError: ... credential ...` |
| попытка bind после `CANCEL_REQUESTED` | `ValueError: supervised run is CANCEL_REQUESTED` |

Во всех случаях ledger остаётся без изменений.