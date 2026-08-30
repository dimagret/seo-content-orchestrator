# Supervised subscription rail — failure states

Документ описывает **только локальное** поведение supervised rail в нештатных ситуациях. Никакой upstream-отмены не предполагается; все state-переходы выполняются локальным operator-управляемым путём.

## `OPERATOR_RECOVERY_REQUIRED`

### Когда возникает

- Hermes-сессия завершилась аварийно до того, как оператор смог наблюдать результат;
- оператор не уверен, выполнился ли stage в видимой сессии;
- supervised-bind обнаружил, что completion не соответствует outstanding packet.

### Что запрещено

- автоматически создавать новый пакет;
- автоматически повторять стадию;
- предполагать, что upstream provider уже выполнил запрос.

### Что оператор делает

1. Запускает `supervised-status` для подтверждения статуса.
2. Смотрит `supervised-events` ledger (через read-only SQL или специальный read-only CLI helper).
3. Решает один из двух исходов:

   - **resolve для того же binding**: вызывает `supervised-bind` с флагом `--resolve-recovery` и тем же `input_hash`. После этого binding повторяется на тех же identity-параметрах. Никаких новых packets, никаких новых sequence.

   - **cancel**: вызывает операторский cancel-протокол (см. ниже).

### Что записывается в ledger

Событие `RECOVERY_RESOLVED_FOR_EXACT_BINDING`:

```text
operator_id, outstanding_input_hash,
replacement_attempt=false, maximum_retries=0
```

Если `operator_id` не совпадает с предыдущим — ledger отвергает событие.

## `CANCEL_REQUESTED`

### Что означает

Локальное намерение оператора **не продолжать** supervised rail для этого `(company_id, job_id)`. Это **не** означает, что upstream provider отменил свой запрос. consumer `chatgpt.com/backend-api/codex` не предоставляет доказанного upstream cancel.

### Что запрещено

- записывать `CANCEL_REQUESTED` как `CANCELED`;
- предполагать, что upstream сессия остановлена;
- автоматически пытаться повторно отправить запрос.

### Что оператор делает

1. Вызывает cancel-протокол (отдельный CLI не предоставляется до G4; в текущем supervised API есть `request_cancel`).
2. Запускает `supervised-status` — видит `CANCEL_REQUESTED` и `outstanding_input_hash`.
3. Прекращает попытки supervised-bind.

### Что записывается в ledger

```text
CANCEL_REQUESTED
operator_id, prior_status,
outstanding_input_hash, local_request_only=true, upstream_cancellation=false
```

`local_request_only=true` и `upstream_cancellation=false` — это **обязательные** поля. Если запись их не содержит, ledger отвергает событие.

## Граница с JobService

`CANCEL_REQUESTED` в supervised rail — это **локальное намерение**. Оно **не** переводит `JobService` в `JobState.CANCELED`. Это сделано намеренно:

- supervised rail не знает, что произошло в upstream сессии;
- JobService CANCELED — это необратимый переход в job-level state machine;
- перевод в JobState.CANCELED должен быть явным отдельным owner-decision gate после финального supervised-статуса.

Если потребуется JobService-level cancel, это делается отдельной операцией **после** supervised `CANCEL_REQUESTED`, с явной записью в JobService `transitions` и без автоматического повтора.

## No-auto-retry

`maximum_retries=0` зафиксирован на уровне execution plan и подтверждается в `_validate_plan`:

```python
plan.maximum_retries != 0 → ValueError("approved execution plan is not the supervised subscription plan")
```

Это означает:

- никаких retry budgetов в `Runner`;
- никаких фоновых recovery-воркеров;
- никаких автоматических повторных supervised-bind;
- каждое решение — отдельный операторский gate.