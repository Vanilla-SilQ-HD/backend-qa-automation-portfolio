from datetime import UTC, datetime

from hamcrest import assert_that, equal_to, none, not_none

import config
from account_service.portfolio_accounts_transactions_message import PortfolioAccountsTransactionsMessage
from helpers import check_assert_that
from helpers.allure_client import allure_attach
from kafka_topics.internal import BaseTopic


class PortfolioAccountsTransactionsTopic(BaseTopic):
    expected_header_event_type = None

    def __init__(self):
        self.name = config.accounts_transactions_topic

    def _check_key(self, message, expected_account_id: str):
        check_assert_that(message.key, equal_to(expected_account_id), "Kafka key должен совпадать с accountId")

    def _check_headers(self, message, expected_header_event_type: str | None = None):
        resolved = (
            expected_header_event_type
            if expected_header_event_type is not None
            else self.expected_header_event_type
        )
        headers = self._get_headers_from_message(message)
        event_type = headers.get("eventType")
        check_assert_that(event_type, not_none(), "В headers должно присутствовать поле eventType")
        if resolved is not None:
            check_assert_that(
                event_type,
                equal_to(resolved),
                "Некорректный eventType в headers",
            )
        return headers

    def set_message_bonus_exchange_completed(
        self,
        *,
        operation_id: int,
        account_id: str,
        client_id: str,
        amount: int,
        currency: int,
        account_type_id: int,
        event_time: str | None = None,
        transaction_id: int | None = None,
        transaction_type_id: int = 2,
        transaction_event_id: int = 7774,
        transaction_status_id: int = 1,
        description: str = 'Обмен бонусов',
    ):
        """Исходящее сообщение Portfolio.Accounts.Transactions (bonusExchange, успешное списание)."""
        et = event_time or datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
        tid = transaction_id or int(datetime.now(UTC).timestamp() * 1000)
        value = {
            'transactionId': tid,
            'operationId': operation_id,
            'accountId': account_id,
            'accountTypeId': account_type_id,
            'clientId': client_id,
            'eventTime': et,
            'currency': currency,
            'amount': amount,
            'type': transaction_type_id,
            'event': transaction_event_id,
            'status': transaction_status_id,
            'description': description,
            'createdAt': et,
        }
        PortfolioAccountsTransactionsMessage.model_validate(value)
        headers = self._set_headers()
        headers.append(('eventType', 'bonusExchange'.encode()))
        key = account_id
        return value, key, headers

    def check_message(
        self,
        message,
        expected_account_id: str,
        expected_client_id: str,
        expected_account_type_id: int,
        expected_currency: int,
        expected_amount: int,
        expected_type_id: int,
        expected_event_id: int,
        expected_status_id: int,
        expected_description: str,
        expected_operation_id: int | None = None,
        expected_header_event_type: str | None = None,
        expected_event_time: str | None = None,
    ):
        assert_that(
            message,
            not_none(),
            "Сообщение в Portfolio.Accounts.Transactions не найдено по текущему eventTime/accountId/clientId",
        )
        assert_that(message.topic, equal_to(self.name), "Ошибка в названии топика")
        self._check_key(message, expected_account_id)

        headers = self._check_headers(message, expected_header_event_type=expected_header_event_type)
        allure_attach(headers, "portfolio_accounts_transactions_headers")

        parsed = PortfolioAccountsTransactionsMessage.model_validate(message.value)
        allure_attach(parsed.model_dump(mode="json"), "portfolio_accounts_transactions_parsed")
        assert_that(parsed.transactionId, not_none(), "transactionId должен быть заполнен в контракте Kafka")
        if expected_operation_id is None:
            assert_that(parsed.operationId, none(), "operationId должен быть null для текущего сценария")
        else:
            assert_that(parsed.operationId, equal_to(expected_operation_id), "Некорректный operationId в Kafka body")
        assert_that(parsed.accountId, equal_to(expected_account_id), "Некорректный accountId в Kafka body")
        assert_that(parsed.accountTypeId, equal_to(expected_account_type_id), "Некорректный accountTypeId в Kafka body")
        assert_that(parsed.clientId, equal_to(expected_client_id), "Некорректный clientId в Kafka body")
        if expected_event_time is not None:
            assert_that(parsed.eventTime, equal_to(expected_event_time), "Некорректный eventTime в Kafka body")
        assert_that(parsed.currency, equal_to(expected_currency), "Некорректный currency в Kafka body")
        assert_that(parsed.amount, equal_to(expected_amount), "Некорректный amount в Kafka body")
        assert_that(parsed.type_, equal_to(expected_type_id), "Некорректный type в Kafka body")
        assert_that(parsed.event, equal_to(expected_event_id), "Некорректный event в Kafka body")
        assert_that(parsed.status, equal_to(expected_status_id), "Некорректный status в Kafka body")
        assert_that(parsed.description, equal_to(expected_description), "Некорректный description в Kafka body")
        assert_that(parsed.createdAt, not_none(), "createdAt должен присутствовать в Kafka body")
