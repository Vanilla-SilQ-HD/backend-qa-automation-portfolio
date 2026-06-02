# Backend QA Automation Portfolio

[English](README.md) | [Русский](README_RU.md)

Synthetic portfolio project demonstrating backend QA automation patterns.

## Domain

**Reward Ledger Demo** is an intentionally synthetic event-driven domain for reward ledger checks. The examples focus on backend test design rather than a specific product or organization.

## What This Repository Demonstrates

- Python and pytest test organization;
- REST API testing;
- Kafka and event-driven testing;
- SQLAlchemy database assertions;
- Pydantic contract validation;
- Allure-style test structure;
- an end-to-end reward accrual flow;
- test data preparation and cleanup.

## Important Note

This is a code portfolio, not a standalone runnable framework. Examples are intentionally synthetic and do not include production infrastructure, credentials, real endpoints or proprietary helper layers.

This repository does not reproduce proprietary systems, names, schemas, tickets, production flows, credentials or internal infrastructure.

## Repository Structure

- `examples/account_service` - Member Ledger examples.
- `examples/bonus_service` - Reward Ledger examples.
- `examples/operation_service` - Reward Workflow examples.
- `examples/e2e` - E2E Reward Ledger flow.
- `templates/config_public.py` - safe configuration template.
- `MANIFEST.md` - test file index.

## Recommended Files to Review

1. `examples/e2e/bonus_accrual_after_balance_end_of_day/e2e/test_balance_end_of_day_bonus_accrual_e2e.py` - event-driven E2E flow with Kafka, database checks and reward activity API validation.
2. `examples/account_service/operations_events_negative_consumer/account_service/test_operations_events_consumer.py` - negative consumer scenario with prepared database state and transaction checks.
3. `examples/bonus_service/operations_events_consumer/bonus_service/test_operations_events_consumer.py` - Kafka consumer scenarios with reward balance validation.
4. `examples/bonus_service/bonus_accruals_api/bonus_service/test_bonus_accruals_get.py` - REST API checks with response model validation.
