import time
from datetime import UTC, datetime, timedelta

from hamcrest import assert_that, equal_to, less_than, not_none

from history_service import requests_session
from history_service.response_models.history_by_client_get import HistoryByClientGetResponse
from helpers.allure_client import allure, allure_attach

from e2e.bonus_accrual_flow import (
    BONUS_ACCOUNT_TYPE_ID,
    BONUS_CURRENCY_ID,
    CLIENT_ID,
    TRANSACTION_EVENT_ID,
    parse_event_time,
)


def parse_history_operation_date(value) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(UTC)
        return value.replace(tzinfo=UTC)
    if isinstance(value, str) and value.endswith('Z'):
        value = value[:-1] + '+00:00'
    return datetime.fromisoformat(value).astimezone(UTC)


class HistoryAssertions:
    def __init__(self, context, flow):
        self.context = context
        self.flow = flow

    def assert_history(self):
        c = self.context
        self.flow.wait_transaction_message()
        with allure.step('Проверяем GET /v1/history/byClient'):
            self._assert_history_by_client(c.amount, c.tx_id, c.event_time, c.bonus_account_id)

    def _assert_history_by_client(self, amount: int, tx_id, event_time: str, bonus_account_id: str):
        event_time_utc = parse_event_time(event_time).astimezone(UTC)
        params = {
            'clientId': CLIENT_ID,
            'dateFrom': (event_time_utc - timedelta(days=1)).isoformat(),
            'dateTo': (datetime.now(UTC) + timedelta(days=1)).isoformat(),
            'count': 100,
            'page': 1,
        }

        matched = self._wait_history_operation(params, amount, tx_id, bonus_account_id)
        if matched is None:
            with allure.step('Дополнительная проверка записи в history БД'):
                row = self._find_history_operation_row(amount, tx_id, event_time, bonus_account_id)
                allure_attach(row, 'history_db_fallback_row')
                if row is None:
                    self._attach_operations_snapshot(event_time)
            assert_that(matched, not_none(), 'Операция не найдена в API GET /v1/history/byClient')

        self._assert_history_operation(matched, amount, tx_id, bonus_account_id, event_time_utc)

    def _wait_history_operation(self, params, amount: int, tx_id, bonus_account_id: str):
        matched = None
        last_response = None
        attached = False
        with allure.step('Ожидаем операцию в истории'):
            for attempt in range(60):
                response = requests_session.get(f'{requests_session.url}v1/history/byClient', params=params)
                last_response = response
                if response.status_code not in (200, 404):
                    allure_attach(response, 'history_byClient_response')
                    attached = True
                    break
                if response.status_code == 404:
                    if attempt < 59:
                        time.sleep(2)
                    continue

                parsed = HistoryByClientGetResponse.model_validate(response.json())
                matched = self._find_matching_history_operation(parsed.operations, amount, tx_id, bonus_account_id)
                if matched is not None:
                    allure_attach(response, 'history_byClient_response')
                    attached = True
                    break
                if attempt < 59:
                    time.sleep(2)

            if not attached and last_response is not None:
                allure_attach(last_response, 'history_byClient_response')
        return matched

    @staticmethod
    def _find_matching_history_operation(operations, amount: int, tx_id, bonus_account_id: str):
        for operation in operations:
            tx_matches = tx_id is not None and str(operation.transactionId) == str(tx_id)
            if tx_matches and (
                str(operation.accountId) == str(bonus_account_id)
                and operation.accountType == BONUS_ACCOUNT_TYPE_ID
                and operation.amount == amount
                and operation.currency == BONUS_CURRENCY_ID
            ):
                return operation
        return None

    def _find_history_operation_row(self, amount: int, tx_id, event_time: str, bonus_account_id: str):
        c = self.context
        m = c.operation_db.model
        event_time_utc = parse_event_time(event_time).astimezone(UTC)
        window_start = event_time_utc - timedelta(minutes=5)
        window_end = event_time_utc + timedelta(minutes=10)

        if tx_id is not None:
            row = (
                c.operation_db.session.query(m.Operation)
                .filter(m.Operation.ExtId == str(tx_id))
                .order_by(m.Operation.ModifiedOn.desc())
                .first()
            )
            if row is not None:
                return row

        return (
            c.operation_db.session.query(m.Operation)
            .filter(m.Operation.ClientId == CLIENT_ID)
            .filter(m.Operation.AccountId == bonus_account_id)
            .filter(m.Operation.Amount == amount)
            .filter(m.Operation.Currency == BONUS_CURRENCY_ID)
            .filter(m.Operation.ModifiedOn >= window_start)
            .filter(m.Operation.ModifiedOn <= window_end)
            .order_by(m.Operation.ModifiedOn.desc())
            .first()
        )

    def _attach_operations_snapshot(self, event_time: str):
        c = self.context
        m = c.operation_db.model
        event_time_utc = parse_event_time(event_time).astimezone(UTC)
        window_start = event_time_utc - timedelta(minutes=10)
        window_end = event_time_utc + timedelta(minutes=10)
        rows = (
            c.operation_db.session.query(m.Operation)
            .filter(m.Operation.ClientId == CLIENT_ID)
            .filter(m.Operation.ModifiedOn >= window_start)
            .filter(m.Operation.ModifiedOn <= window_end)
            .order_by(m.Operation.ModifiedOn.desc())
            .limit(50)
            .all()
        )
        allure_attach(
            {
                'note': 'Операции клиента в окне +/-10 минут вокруг event_time',
                'clientId': CLIENT_ID,
                'event_time': event_time,
                'window_utc_from': window_start.isoformat(),
                'window_utc_to': window_end.isoformat(),
                'rows_returned': len(rows),
                'operations': [self._operation_to_dict(row) for row in rows],
            },
            'operations_snapshot',
        )

    @staticmethod
    def _operation_to_dict(row):
        return {
            'Id': row.Id,
            'ExtId': str(row.ExtId) if row.ExtId is not None else None,
            'ClientId': str(row.ClientId),
            'AccountId': str(row.AccountId) if row.AccountId is not None else None,
            'Amount': row.Amount,
            'Currency': row.Currency,
            'Description': row.Description,
            'OperationStateId': row.OperationStateId,
            'OperationTypeId': row.OperationTypeId,
            'CreatedOn': row.CreatedOn.isoformat() if row.CreatedOn is not None else None,
            'ModifiedOn': row.ModifiedOn.isoformat() if row.ModifiedOn is not None else None,
        }

    @staticmethod
    def _assert_history_operation(operation, amount: int, tx_id, bonus_account_id: str, event_time_utc: datetime):
        assert_that(operation.transactionId, equal_to(tx_id), 'Некорректный transactionId в history response')
        assert_that(str(operation.accountId), equal_to(str(bonus_account_id)), 'Некорректный accountId в history response')
        assert_that(operation.accountType, equal_to(BONUS_ACCOUNT_TYPE_ID), 'Некорректный accountType в history response')
        assert_that(operation.amount, equal_to(amount), 'Некорректный amount в history response')
        assert_that(operation.currency, equal_to(BONUS_CURRENCY_ID), 'Некорректный currency в history response')
        assert_that(operation.operationState.name, equal_to('Success'), 'Некорректный operationState.name в history response')
        assert_that(operation.operationType.id, equal_to(TRANSACTION_EVENT_ID), 'Некорректный operationType.id в history response')
        assert_that(operation.operationType.name, equal_to('bonusAccrualForBalance'), 'Некорректный operationType.name в history response')
        assert_that(operation.operationDate, not_none(), 'operationDate должна присутствовать в history response')
        assert_that(
            abs((parse_history_operation_date(operation.operationDate) - event_time_utc).total_seconds()),
            less_than(120),
            'operationDate должна быть близка к event_time',
        )
