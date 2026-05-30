from typing import Optional

from hamcrest import assert_that, equal_to, matches_regexp, not_none
from kafka.consumer.fetcher import ConsumerRecord

import config
from helpers import check_assert_that
from helpers.pydantic_base_model import BasePydanticModel
from kafka_topics.internal import BaseTopic


# Portfolio.Bonus.Operation

class Trace(BasePydanticModel):
    notifyId: str
    requestId: Optional[str] = None
    requestTime: Optional[str] = None


class BonusPackage(BasePydanticModel):
    bonusId: int
    amountUsed: int
    currency: Optional[int] = None
    percentage: Optional[float] = None
    status: Optional[int] = None
    operationCategory: Optional[int] = None


class BonusOperation(BasePydanticModel):
    clientId: str
    accountId: Optional[str] = None
    accountType: Optional[int] = None
    msisdn: Optional[str] = None
    eventType: int
    eventTime: str
    amount: int
    operationDate: str
    operationId: Optional[int] = None
    bonusPackages: list[BonusPackage]


class Message(BasePydanticModel):
    trace: Trace
    bonusOperation: BonusOperation


class PortfolioBonusOperationTopic(BaseTopic):
    """
    Producers: BonusService
    Consumers: AccountService
    Topic: Portfolio.Bonus.Operation (eventType=bonusExchange)
    """

    def __init__(self, client_id, account_id, operation_id, amount,
                 event_type, bonus_packages):
        self.name = config.bonus_operation_topic
        self._client_id = client_id
        self._account_id = account_id
        self._operation_id = operation_id
        self._amount = amount
        self._event_type = event_type
        self._bonus_packages = bonus_packages

    def _check_key(self, message: ConsumerRecord):
        check_assert_that(message.key, not_none(), 'Ошибка в ключе')
        check_assert_that(message.key, equal_to(str(self._client_id)), 'Ошибка в ключе')

    def _check_headers(self, message: ConsumerRecord):
        headers = self._get_headers_from_message(message)
        check_assert_that(headers.get('ProducerName'), equal_to('Portfolio.BonusService.Api'),
                          'Ошибка в ProducerName')
        check_assert_that(headers.get('X-Correlation-ID'), not_none(), 'Ошибка в X-Correlation-ID')
        check_assert_that(headers.get('X-Correlation-ID'),
                          matches_regexp('^[0-9a-f]{32}$'), 'Ошибка в X-Correlation-ID')
        check_assert_that(headers.get('eventType'), equal_to('bonusExchange'), 'Ошибка в eventType')

    def check_message(self, message: ConsumerRecord):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')

        self._check_key(message)
        self._check_headers(message)

        msg = Message(**message.value)
        check_assert_that(msg.trace.notifyId, equal_to('bonusExchange'), 'Ошибка в trace.notifyId')
        check_assert_that(msg.trace.requestId, not_none(), 'Ошибка в trace.requestId')
        check_assert_that(msg.trace.requestTime, not_none(), 'Ошибка в trace.requestTime')

        operation = msg.bonusOperation
        check_assert_that(operation.clientId, equal_to(str(self._client_id)), 'Ошибка в clientId')
        check_assert_that(operation.accountId, equal_to(str(self._account_id)), 'Ошибка в accountId')
        check_assert_that(operation.operationId, equal_to(self._operation_id), 'Ошибка в operationId')
        check_assert_that(operation.eventType, equal_to(self._event_type), 'Ошибка в eventType')
        check_assert_that(operation.amount, equal_to(self._amount), 'Ошибка в amount')
        check_assert_that(operation.eventTime, not_none(), 'Ошибка в eventTime')
        check_assert_that(len(operation.bonusPackages), equal_to(len(self._bonus_packages)),
                          'Ошибка в количестве bonusPackages')
        check_assert_that(sum(package.amountUsed for package in operation.bonusPackages),
                          equal_to(self._amount), 'Сумма amountUsed должна совпадать с amount')

        actual_packages = {package.bonusId: package for package in operation.bonusPackages}
        for expected in self._bonus_packages:
            package = actual_packages.get(expected['bonus_id'])
            check_assert_that(package, not_none(),
                              f"Пакет bonusId={expected['bonus_id']} отсутствует в сообщении")
            if package is None:
                continue
            check_assert_that(package.amountUsed, equal_to(expected['amount_used']),
                              f"Ошибка в amountUsed для bonusId={expected['bonus_id']}")
            check_assert_that(package.currency, equal_to(expected['currency']),
                              f"Ошибка в currency для bonusId={expected['bonus_id']}")
            check_assert_that(package.percentage, equal_to(float(expected['percentage'])),
                              f"Ошибка в percentage для bonusId={expected['bonus_id']}")
            check_assert_that(package.status, equal_to(expected['status']),
                              f"Ошибка в status для bonusId={expected['bonus_id']}")
