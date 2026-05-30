from hamcrest import assert_that, equal_to, none, not_none
from pydantic import ConfigDict, Field

import config
from helpers import check_assert_that
from helpers.allure_client import allure_attach
from helpers.pydantic_base_model import BasePydanticModel
from kafka_topics.internal import BaseTopic


class PortfolioAccountsTransactionsMessage(BasePydanticModel):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)

    transactionId: int
    operationId: int | None = None
    accountId: str
    accountTypeId: int | None = None
    clientId: str
    eventTime: str
    currency: int
    amount: int
    type_: int = Field(alias='type')
    event: int
    status: int
    description: str
    createdAt: str | None = None


class PortfolioAccountsTransactionsTopic(BaseTopic):
    expected_header_event_type = 'bonusAccrualForBalance'

    def __init__(self):
        self.name = config.accounts_transactions_topic

    @staticmethod
    def _decode_key(value):
        if isinstance(value, bytes):
            return value.decode()
        return value

    def check_message(
        self,
        message,
        expected_account_id,
        expected_client_id,
        expected_event_time,
        expected_account_type_id,
        expected_currency,
        expected_amount,
        expected_type_id,
        expected_event_id,
        expected_status_id,
        expected_description,
    ):
        assert_that(
            message,
            not_none(),
            'Сообщение в Portfolio.Accounts.Transactions не найдено по eventTime/accountId/clientId',
        )
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')
        check_assert_that(
            self._decode_key(message.key),
            equal_to(expected_account_id),
            'Kafka key должен совпадать с accountId',
        )

        headers = self._get_headers_from_message(message)
        check_assert_that(
            headers.get('eventType'),
            equal_to(self.expected_header_event_type),
            'В headers.eventType ожидается bonusAccrualForBalance',
        )
        allure_attach(headers, 'portfolio_accounts_transactions_headers')

        parsed = PortfolioAccountsTransactionsMessage.model_validate(message.value)
        allure_attach(parsed.model_dump(mode='json'), 'portfolio_accounts_transactions_parsed')

        assert_that(parsed.transactionId, not_none(), 'transactionId должен быть заполнен в контракте Kafka')
        assert_that(parsed.operationId, none(), 'operationId должен быть null')
        assert_that(parsed.accountId, equal_to(expected_account_id), 'Некорректный accountId в Kafka body')
        if parsed.accountTypeId is not None:
            assert_that(
                parsed.accountTypeId,
                equal_to(expected_account_type_id),
                'Некорректный accountTypeId в Kafka body',
            )
        assert_that(parsed.clientId, equal_to(expected_client_id), 'Некорректный clientId в Kafka body')
        assert_that(parsed.eventTime, equal_to(expected_event_time), 'Некорректный eventTime в Kafka body')
        assert_that(parsed.currency, equal_to(expected_currency), 'Некорректный currency в Kafka body')
        assert_that(parsed.amount, equal_to(expected_amount), 'Некорректный amount в Kafka body')
        assert_that(parsed.type_, equal_to(expected_type_id), 'Некорректный type в Kafka body')
        assert_that(parsed.event, equal_to(expected_event_id), 'Некорректный event в Kafka body')
        assert_that(parsed.status, equal_to(expected_status_id), 'Некорректный status в Kafka body')
        assert_that(
            parsed.description,
            equal_to(expected_description),
            'Некорректный description в Kafka body',
        )
        return parsed
