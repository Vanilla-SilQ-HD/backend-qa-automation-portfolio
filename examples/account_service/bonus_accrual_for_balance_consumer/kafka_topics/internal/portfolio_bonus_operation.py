from typing import Optional

from hamcrest import assert_that, equal_to, none, not_none
from kafka.consumer.fetcher import ConsumerRecord

import config
from helpers import check_assert_that, data_generator, datetime_now, get_test_uuid
from helpers.pydantic_base_model import BasePydanticModel, uuid_str
from kafka_topics.internal import BaseTopic


# Portfolio.Bonus.Operation

class Trace(BasePydanticModel):
    notifyId: str
    requestId: uuid_str
    requestTime: str


class BonusPackage(BasePydanticModel):
    bonusId: int
    currency: int
    percentage: float
    status: int
    amountUsed: int


class BonusOperation(BasePydanticModel):
    bonusPackages: list[BonusPackage]
    accountId: Optional[uuid_str] = None
    accountType: Optional[int] = None
    clientId: uuid_str
    eventType: int
    eventTime: str
    amount: int
    operationDate: str
    operationId: Optional[int] = None


class Message(BasePydanticModel):
    trace: Trace
    bonusOperation: BonusOperation


datetime_format = '%Y-%m-%dT%H:%M:%SZ'


class PortfolioBonusOperationTopic(BaseTopic):
    """
    Producers: BonusService
    Consumers: AccountService

    eventType:
      bonusAccrualForBalance - начисление бонусов на накоп
      bonusUnfrozen          - разморозка бонусов
    """

    def __init__(self, client: data_generator.Client, event_type, amount,
                 bonus_id=None, account_id=None, currency=None, percentage=None,
                 request_id=None, request_time=None, event_time=None):
        self.name = config.bonus_operation_topic
        self._client = client
        self._event_type = event_type
        self._amount = amount
        self._bonus_id = bonus_id if bonus_id is not None else int(datetime_now().timestamp() * 1000)
        self._currency = currency if currency is not None else 9991  # Бонусы
        self._percentage = percentage if percentage is not None else 13.0
        self._request_id = request_id or get_test_uuid()
        self._request_time = request_time or datetime_now().strftime(datetime_format)
        self._event_time = event_time or datetime_now().strftime(datetime_format)
        if event_type == 'bonusAccrualForBalance':
            self._body_event_type = 1
            self._bonus_status = 0  # FROZEN
            self._account_id = account_id or get_test_uuid()  # накопительный счёт
        else:  # bonusUnfrozen
            self._body_event_type = 7772
            self._bonus_status = 1  # ACTIVE
            self._account_id = account_id

    def set_message(self):
        value = {
            'trace': {
                'notifyId': self._event_type,
                'requestId': self._request_id,
                'requestTime': self._request_time,
            },
            'bonusOperation': {
                'bonusPackages': [{
                    'bonusId': self._bonus_id,
                    'currency': self._currency,
                    'percentage': self._percentage,
                    'status': self._bonus_status,
                    'amountUsed': self._amount,
                }],
                'accountId': str(self._account_id) if self._account_id is not None else None,
                'accountType': None,
                'clientId': str(self._client.id),
                'eventType': self._body_event_type,
                'eventTime': self._event_time,
                'amount': self._amount,
                'operationDate': self._event_time,
                'operationId': None,
            },
        }
        Message(**value)

        key = str(self._client.id)
        headers = self._set_headers()
        headers.append(('eventType', self._event_type.encode()))
        return value, key, headers

    def _check_key(self, message: ConsumerRecord):
        check_assert_that(message.key, not_none(), 'Ошибка в ключе')
        check_assert_that(message.key, equal_to(str(self._client.id)), 'Ошибка в ключе')

    def _check_message_headers(self, message: ConsumerRecord):
        headers = self._get_headers_from_message(message)
        check_assert_that(headers.get('eventType'), equal_to(self._event_type),
                          'Ошибка в eventType заголовка')

    def check_message(self, message: ConsumerRecord):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')

        self._check_key(message)
        self._check_message_headers(message)

        msg = Message(**message.value)
        check_assert_that(msg.trace.notifyId, equal_to(self._event_type), 'Ошибка в trace.notifyId')
        check_assert_that(msg.trace.requestId, not_none(), 'Ошибка в trace.requestId')
        check_assert_that(msg.trace.requestTime, not_none(), 'Ошибка в trace.requestTime')

        operation = msg.bonusOperation
        check_assert_that(operation.clientId, equal_to(str(self._client.id)), 'Ошибка в clientId')
        check_assert_that(operation.eventType, equal_to(self._body_event_type), 'Ошибка в eventType')
        check_assert_that(operation.eventTime, not_none(), 'Ошибка в eventTime')
        check_assert_that(operation.amount, equal_to(self._amount), 'Ошибка в amount')
        check_assert_that(operation.accountId, equal_to(self._account_id), 'Ошибка в accountId')
        check_assert_that(operation.accountType, none(), 'Ошибка в accountType')
        check_assert_that(operation.operationId, none(), 'Ошибка в operationId')

        package = operation.bonusPackages[0]
        check_assert_that(package.bonusId, equal_to(self._bonus_id), 'Ошибка в bonusId')
        check_assert_that(package.status, equal_to(self._bonus_status), 'Ошибка в status')
        check_assert_that(package.amountUsed, equal_to(self._amount), 'Ошибка в amountUsed')
        check_assert_that(package.currency, equal_to(self._currency), 'Ошибка в currency')
        check_assert_that(package.percentage, equal_to(self._percentage), 'Ошибка в percentage')
