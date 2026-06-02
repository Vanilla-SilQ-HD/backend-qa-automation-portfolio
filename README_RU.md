# Backend QA Automation Portfolio

[English](README.md) | [Русский](README_RU.md)

Синтетический portfolio-проект, который демонстрирует подходы к автоматизации тестирования backend-систем.

## Предметная область

**Reward Ledger Demo** - намеренно синтетическая event-driven модель для проверки учёта reward units. Примеры показывают устройство backend-автотестов и не привязаны к конкретному продукту или организации.

## Что показывает репозиторий

- организацию тестов на Python и pytest;
- тестирование REST API;
- проверку Kafka-событий и асинхронных сценариев;
- проверки состояния БД через SQLAlchemy;
- валидацию контрактов через Pydantic;
- структуру тестов в стиле Allure;
- end-to-end сценарий начисления reward units;
- подготовку и очистку тестовых данных.

## Важно

Это портфолио с примерами кода, а не готовый standalone-фреймворк для запуска. Примеры намеренно синтетические: в них нет production-инфраструктуры, учётных данных, реальных endpoints или закрытых вспомогательных библиотек.

Репозиторий не воспроизводит закрытые системы, названия, схемы, тикеты, production-сценарии, учётные данные или внутреннюю инфраструктуру.

## Структура репозитория

- `examples/account_service` - примеры Member Ledger.
- `examples/bonus_service` - примеры Reward Ledger.
- `examples/operation_service` - примеры Reward Workflow.
- `examples/e2e` - E2E-сценарий Reward Ledger.
- `templates/config_public.py` - безопасный шаблон конфигурации.
- `MANIFEST.md` - индекс тестовых файлов.

## Рекомендуемые файлы для просмотра

1. `examples/e2e/bonus_accrual_after_balance_end_of_day/e2e/test_balance_end_of_day_bonus_accrual_e2e.py` - event-driven E2E-сценарий с Kafka, проверками БД и reward activity API.
2. `examples/account_service/operations_events_negative_consumer/account_service/test_operations_events_consumer.py` - негативный consumer-сценарий с подготовкой состояния БД и проверкой транзакций.
3. `examples/bonus_service/operations_events_consumer/bonus_service/test_operations_events_consumer.py` - Kafka consumer-сценарии с проверкой reward balance.
4. `examples/bonus_service/bonus_accruals_api/bonus_service/test_bonus_accruals_get.py` - проверки REST API с валидацией response models.
