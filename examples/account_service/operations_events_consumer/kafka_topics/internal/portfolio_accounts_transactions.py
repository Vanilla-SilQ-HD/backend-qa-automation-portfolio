from typing import Optional

from hamcrest import assert_that, equal_to, matches_regexp, not_none
from kafka.consumer.fetcher import ConsumerRecord
from pydantic import Field

import config
from helpers import check_assert_that
from helpers.pydantic_base_model import BasePydanticModel, uuid_str
from kafka_topics.internal import BaseTopic


class Message(BasePydanticModel):
    transactionId: int
    operationId: Optional[int] = None
    accountId: uuid_str
    accountTypeId: int
    clientId: uuid_str
    eventTime: str
    currency: int
    amount: int
    type_: int = Field(alias='type')
    event: int
    status: int
    description: str
    createdAt: str


class PortfolioAccountsTransactionsTopic(BaseTopic):
    """
    Producers: AccountService
    Consumers: HistoryService
    Topic: Portfolio.Accounts.Transactions
    """

    def __init__(self, transaction, account_type_id, event_type):
        self.name = config.accounts_transactions_topic
        self._transaction = transaction
        self._account_type_id = account_type_id
        self._event_type = event_type

    def _check_key(self, message: ConsumerRecord):
        check_assert_that(message.key, not_none(), 'Ошибка в ключе')
        check_assert_that(message.key, equal_to(str(self._transaction.AccountId)), 'Ошибка в ключе')

    def _check_headers(self, message: ConsumerRecord, producer_name):
        # traceparent в этом топике не передается
        headers = self._get_headers_from_message(message)
        check_assert_that(headers.get('ProducerName'), equal_to(producer_name), 'Ошибка в ProducerName')
        check_assert_that(headers.get('X-Correlation-ID'), not_none(), 'Ошибка в X-Correlation-ID')
        check_assert_that(headers.get('X-Correlation-ID'),
                          matches_regexp('^[0-9a-f]{32}$'), 'Ошибка в X-Correlation-ID')
        check_assert_that(headers.get('eventType'), equal_to(self._event_type), 'Ошибка в eventType')

    def check_message(self, message: ConsumerRecord):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')

        self._check_key(message)
        self._check_headers(message, producer_name='Portfolio.AccountService.Api')

        msg = Message(**message.value)
        check_assert_that(msg.transactionId, equal_to(self._transaction.Id), 'Ошибка в transactionId')
        check_assert_that(msg.operationId, equal_to(self._transaction.OperationId), 'Ошибка в operationId')
        check_assert_that(msg.accountId, equal_to(self._transaction.AccountId), 'Ошибка в accountId')
        check_assert_that(msg.accountTypeId, equal_to(self._account_type_id), 'Ошибка в accountTypeId')
        check_assert_that(msg.clientId, equal_to(self._transaction.ClientId), 'Ошибка в clientId')
        check_assert_that(msg.currency, equal_to(self._transaction.CurrencyId), 'Ошибка в currency')
        check_assert_that(msg.amount, equal_to(self._transaction.Amount), 'Ошибка в amount')
        check_assert_that(msg.type_, equal_to(self._transaction.TypeId), 'Ошибка в type')
        check_assert_that(msg.event, equal_to(self._transaction.EventId), 'Ошибка в event')
        check_assert_that(msg.status, equal_to(1), 'Ошибка в status')
        check_assert_that(msg.description, equal_to(self._transaction.Description), 'Ошибка в description')
        check_assert_that(msg.eventTime, not_none(), 'eventTime должен присутствовать')
