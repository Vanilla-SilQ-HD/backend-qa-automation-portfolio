# Backend QA Automation Portfolio

Портфолио с примерами автотестов для backend-сервисов.

В репозитории собраны обезличенные pytest/allure-тесты из рабочих задач: Kafka consumer/producer сценарии, REST API проверки, работа с БД через SQLAlchemy и e2e-сценарий между несколькими сервисами.

## Что показано

- подготовка тестовых данных в БД;
- отправка и проверка Kafka-сообщений;
- проверки REST API и pydantic response models;
- валидация бизнес-состояний в БД;
- позитивные и негативные сценарии;
- cleanup тестовых данных;
- e2e-проверка начисления бонусов с участием Kafka, БД и history API.

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

- `examples/account_service` - тесты Account Service.
- `examples/bonus_service` - тесты Bonus Service.
- `examples/operation_service` - тесты Operation Service.
- `examples/payment_gateway_adapter_service` - тесты Payment Gateway Adapter Service.
- `examples/e2e` - e2e-сценарий.
- `templates/config_public.py` - пример безопасной конфигурации.
- `MANIFEST.md` - список тестовых файлов.

## Рекомендуемые примеры для просмотра

1. `examples/e2e/bonus_accrual_after_balance_end_of_day/e2e/test_balance_end_of_day_bonus_accrual_e2e.py`
2. `examples/account_service/operations_events_negative_consumer/account_service/test_operations_events_consumer.py`
3. `examples/bonus_service/operations_events_consumer/bonus_service/test_operations_events_consumer.py`
4. `examples/bonus_service/bonus_accruals_api/bonus_service/test_bonus_accruals_get.py`
5. `examples/payment_gateway_adapter_service/callback_payment_status/payment_gateway_adapter_service/test_callback_payment_status.py`

## Важно

Это не standalone-проект для запуска, а портфолио с примерами кода. Реальные домены, доступы, внутренние ссылки, секреты и проектные идентификаторы удалены или заменены на нейтральные значения.

Цель репозитория - показать стиль написания автотестов, декомпозицию сценариев, работу с асинхронными событиями и подход к проверке данных в backend-системах.
