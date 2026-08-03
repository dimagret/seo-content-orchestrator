# SEO Content Orchestrator — Domain Language

## Company

Клиентская организация или бренд, являющийся верхней границей изоляции данных. Данные одной Company никогда не наследуются другой Company.

## Company Profile

Версионируемое описание стабильных фактов о Company: бренд, продукты и услуги, коммерческая модель, география, УТП, доказательства, процессы, tone of voice, допустимые и запрещённые утверждения.

## Company Card

Пользовательское представление Company и её версионируемых Company Profile, Business Directions и Audience Segments. Карточка хранится в orchestrator storage и никогда не является набором n8n nodes.

## Business Direction

Отдельная услуга, продуктовая линия, категория или рыночное направление внутри Company. У одной Company может быть несколько Business Directions с разными предложениями, ценами, кейсами, контекстом и структурой страниц.

## Audience Segment

Версионируемый сегмент целевой аудитории, принадлежащий конкретному Business Direction: роли, задачи, боли, риски, возражения, критерии выбора, бюджет и цикл решения.

## Page Brief

Задание на одну страницу. Явно выбирает Company Profile, Business Direction и Audience Segment и добавляет структуру страницы, ключи, LSI, конкурентов и контекст текущей страницы.

## Manual

Пользовательский термин для полного контекста генерации. В системе Manual не является глобальным редактируемым текстом. Каноническая сущность для запуска — Execution Snapshot.

## Execution Snapshot

Неизменяемая собранная версия Company Profile, Business Direction, Audience Segment, Page Brief и набора prompts. Имеет уникальный hash и передаётся целиком при каждом запуске.

## SEO Job

Одна попытка выполнения на основе одного Execution Snapshot. Job не читает «текущий профиль» или данные предыдущих задач.

## Universal Workflow

Одна параметризованная Stage B копия n8n workflow, обслуживающая все Company. Её nodes меняются только при изменении алгоритма обработки, но не при создании или редактировании Company Card.

## Approval

Разрешение на конкретный snapshot и план. Изменение Company, Direction, Audience, Brief, prompts, моделей, стоимости или назначения делает approval недействительным.
