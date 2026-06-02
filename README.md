# Backend QA Automation Portfolio

Synthetic portfolio project with backend QA automation examples.

Reward Ledger Demo is written from scratch to demonstrate common backend QA automation patterns: Kafka consumer/producer scenarios, REST API checks, SQLAlchemy database assertions and an event-driven end-to-end flow.

## Что показано

- подготовка тестовых данных в БД;
- отправка и проверка Kafka-сообщений;
- проверки REST API и pydantic response models;
- валидация бизнес-состояний в БД;
- позитивные и негативные сценарии;
- cleanup тестовых данных;
- e2e-проверка event-driven начисления reward units с участием Kafka, БД и reward activity API.

## Стек

- Python
- pytest
- Allure
- SQLAlchemy
- Kafka
- REST API
- Pydantic
- Hamcrest

## Структура

- `examples/account_service` - Member Ledger examples.
- `examples/bonus_service` - Reward Ledger examples.
- `examples/operation_service` - Reward Workflow examples.
- `examples/e2e` - E2E Reward Ledger flow.
- `templates/config_public.py` - пример безопасной конфигурации.
- `MANIFEST.md` - список тестовых файлов.

## Рекомендуемые примеры для просмотра

1. `examples/e2e/bonus_accrual_after_balance_end_of_day/e2e/test_balance_end_of_day_bonus_accrual_e2e.py`
2. `examples/account_service/operations_events_negative_consumer/account_service/test_operations_events_consumer.py`
3. `examples/bonus_service/operations_events_consumer/bonus_service/test_operations_events_consumer.py`
4. `examples/bonus_service/bonus_accruals_api/bonus_service/test_bonus_accruals_get.py`

## Важно

Это не standalone-проект для запуска, а synthetic portfolio project с примерами кода для Reward Ledger Demo.

This repository does not reproduce proprietary systems, names, schemas, tickets, production flows, credentials or internal infrastructure.

Цель репозитория - показать стиль написания автотестов, декомпозицию сценариев, работу с асинхронными событиями и подход к проверке данных в backend-системах.
