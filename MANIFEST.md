# Manifest

## Member Ledger examples

Kafka consumer examples for member balances, ledger entries and reward-related account updates.

- `examples/account_service/bonus_accrual_for_balance_consumer/account_service/test_bonus_accrual_for_balance_consumer.py` - 3 теста
- `examples/account_service/bonus_operation_consumer/account_service/test_bonus_operation_consumer.py` - 3 теста
- `examples/account_service/operations_events_consumer/account_service/test_operations_events_consumer.py` - 3 теста
- `examples/account_service/operations_events_negative_consumer/account_service/test_operations_events_consumer.py` - 3 теста

## Reward Ledger examples

Reward ledger examples for balance calculation, accrual APIs, status updates and event processing.

- `examples/bonus_service/balance_end_of_day_consumer/bonus_service/test_balance_end_of_day_consumer.py` - 2 теста
- `examples/bonus_service/bonus_accruals_api/bonus_service/test_bonus_accruals_get.py` - 4 теста
- `examples/bonus_service/bonus_status_change_consumer/bonus_service/test_bonus_status_change_consumer.py` - 2 теста
- `examples/bonus_service/bonus_status_change_job/bonus_service/test_bonus_status_change_job.py` - 1 тест
- `examples/bonus_service/operations_events_consumer/bonus_service/test_operations_events_consumer.py` - 5 тестов
- `examples/bonus_service/operations_events_negative_consumer/bonus_service/test_operations_events_negative_consumer.py` - 3 теста

## Reward Workflow examples

Workflow examples for transaction handling, reward operations and cancellation events.

- `examples/operation_service/accounts_transaction_consumer/operation_service/test_accounts_transaction_consumer.py` - 2 теста
- `examples/operation_service/bonus_operation_consumer/operation_service/test_bonus_operation_consumer.py` - 2 теста
- `examples/operation_service/operations_cancel_consumer/operation_service/test_operations_cancel_consumer.py` - 2 теста

## E2E Reward Ledger flow

End-to-end reward accrual validation across events, database state and API checks.

- `examples/e2e/bonus_accrual_after_balance_end_of_day/e2e/test_balance_end_of_day_bonus_accrual_e2e.py` - 6 тестов
