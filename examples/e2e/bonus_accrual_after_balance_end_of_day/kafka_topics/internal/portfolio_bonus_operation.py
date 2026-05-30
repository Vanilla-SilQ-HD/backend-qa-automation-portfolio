from typing import Optional

from hamcrest import assert_that, equal_to, greater_than, matches_regexp, none, not_none
from pydantic import ConfigDict

import config
from helpers import check_assert_that
from helpers.allure_client import allure_attach
from helpers.pydantic_base_model import BasePydanticModel
from kafka_topics.internal import BaseTopic


class BonusOperationTrace(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    notifyId: str
    requestId: Optional[str] = None
    requestTime: Optional[str] = None


class BonusPackage(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    bonusId: int
    amountUsed: int
    currency: Optional[int] = None
    percentage: Optional[float] = None
    status: Optional[int] = None


class BonusOperationBody(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    clientId: str
    accountId: Optional[str] = None
    accountType: Optional[int] = None
    eventType: int
    eventTime: str
    amount: int
    operationDate: str
    operationId: Optional[int] = None
    bonusPackages: list[BonusPackage]


class BonusOperationMessage(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    trace: BonusOperationTrace
    bonusOperation: BonusOperationBody


class PortfolioBonusOperationTopic(BaseTopic):
    expected_header_event_type = 'BonusUnfrozen'
    expected_header_event_type_accrual = 'bonusAccrualForBalance'

    def __init__(
        self,
        client_id: Optional[str] = None,
        request_id: Optional[str] = None,
        request_time: Optional[str] = None,
        bonus_operation: Optional[dict] = None,
    ):
        self.name = config.bonus_operation_topic
        self.client_id = client_id
        self.request_id = request_id
        self.request_time = request_time
        self.bonus_operation = bonus_operation

    @staticmethod
    def _decode_key(value):
        if isinstance(value, bytes):
            return value.decode()
        return value

    def _check_key(self, message, expected_client_id: str):
        check_assert_that(message.key, not_none(), 'Ошибка в ключе')
        check_assert_that(message.key, equal_to(expected_client_id), 'Ошибка в ключе')

    def _check_headers(self, message):
        headers = self._get_headers_from_message(message)
        check_assert_that(headers.get('ProducerName'), not_none(), 'Ошибка в ProducerName')
        check_assert_that(headers.get('X-Correlation-ID'), not_none(), 'Ошибка в X-Correlation-ID')
        check_assert_that(
            headers.get('X-Correlation-ID'),
            matches_regexp('^[0-9a-f]{32}$'),
            'Ошибка в X-Correlation-ID',
        )
        check_assert_that(
            headers.get('eventType'),
            equal_to(self.expected_header_event_type),
            'В headers.eventType ожидается BonusUnfrozen',
        )
        return headers

    def _check_headers_accrual(self, message):
        headers = self._get_headers_from_message(message)
        check_assert_that(
            headers.get('eventType'),
            equal_to(self.expected_header_event_type_accrual),
            'В headers.eventType ожидается bonusAccrualForBalance',
        )
        return headers

    def check_message(
        self,
        message,
        expected_client_id: str,
        expected_bonus_id: int,
        expected_amount: int,
        expected_currency: int,
        expected_percentage: float,
    ):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')
        self._check_key(message, expected_client_id)

        headers = self._check_headers(message)
        allure_attach(headers, 'portfolio_bonus_operation_headers')

        parsed = BonusOperationMessage.model_validate(message.value)
        allure_attach(parsed.model_dump(mode='json'), 'portfolio_bonus_operation_parsed')

        check_assert_that(parsed.trace.notifyId, equal_to('BonusUnfrozen'), 'Ошибка в trace.notifyId')
        check_assert_that(parsed.trace.requestId, not_none(), 'trace.requestId должен быть заполнен')
        check_assert_that(parsed.trace.requestTime, not_none(), 'trace.requestTime должен быть заполнен')

        op = parsed.bonusOperation
        check_assert_that(op.clientId, equal_to(expected_client_id), 'Ошибка в bonusOperation.clientId')
        check_assert_that(op.eventType, equal_to(2), 'Ошибка в bonusOperation.eventType')
        check_assert_that(op.amount, equal_to(expected_amount), 'Ошибка в bonusOperation.amount')
        check_assert_that(op.operationId, none(), 'bonusOperation.operationId должен быть null')
        check_assert_that(op.accountId, none(), 'bonusOperation.accountId должен быть null')
        check_assert_that(op.accountType, none(), 'bonusOperation.accountType должен быть null')
        check_assert_that(
            len(op.bonusPackages),
            greater_than(0),
            'bonusPackages должен содержать минимум один пакет',
        )

        package = op.bonusPackages[0]
        check_assert_that(package.bonusId, equal_to(expected_bonus_id), 'Ошибка в bonusPackages[0].bonusId')
        check_assert_that(package.status, equal_to(1), 'Ошибка в bonusPackages[0].status')
        check_assert_that(package.amountUsed, equal_to(expected_amount), 'Ошибка в bonusPackages[0].amountUsed')
        check_assert_that(package.currency, equal_to(expected_currency), 'Ошибка в bonusPackages[0].currency')
        check_assert_that(
            package.percentage,
            equal_to(expected_percentage),
            'Ошибка в bonusPackages[0].percentage',
        )

    def check_accrual_for_balance_message(
        self,
        message,
        expected_client_id: str,
        expected_source_account_id: str,
        expected_event_type_id: int,
    ):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')
        check_assert_that(self._decode_key(message.key), equal_to(expected_client_id), 'Ошибка в ключе')

        headers = self._check_headers_accrual(message)
        allure_attach(headers, 'portfolio_bonus_operation_headers')

        parsed = BonusOperationMessage.model_validate(message.value)
        allure_attach(parsed.model_dump(mode='json'), 'portfolio_bonus_operation_parsed')

        check_assert_that(parsed.trace.notifyId, equal_to('bonusAccrualForBalance'), 'Ошибка в trace.notifyId')
        check_assert_that(parsed.trace.requestId, not_none(), 'trace.requestId должен быть заполнен')
        check_assert_that(parsed.trace.requestTime, not_none(), 'trace.requestTime должен быть заполнен')

        op = parsed.bonusOperation
        check_assert_that(op.clientId, equal_to(expected_client_id), 'Ошибка в bonusOperation.clientId')
        check_assert_that(op.accountId, equal_to(expected_source_account_id), 'Ошибка в bonusOperation.accountId')
        check_assert_that(op.eventType, equal_to(expected_event_type_id), 'Ошибка в bonusOperation.eventType')
        check_assert_that(op.operationId, none(), 'bonusOperation.operationId должен быть null')
        check_assert_that(op.accountType, none(), 'bonusOperation.accountType должен быть null')
        check_assert_that(
            len(op.bonusPackages),
            greater_than(0),
            'bonusPackages должен содержать минимум один пакет',
        )
        return parsed

    def set_message(self):
        if not self.client_id or not self.request_id or not self.request_time or not self.bonus_operation:
            raise ValueError('Для set_message должны быть заданы client_id, request_id, request_time и bonus_operation')

        headers = self._set_headers()
        headers.append(('eventType', self.expected_header_event_type.encode()))

        value = {
            'trace': {
                'notifyId': self.expected_header_event_type,
                'requestId': self.request_id,
                'requestTime': self.request_time,
            },
            'bonusOperation': self.bonus_operation,
        }
        BonusOperationMessage.model_validate(value)
        key = self.client_id
        return value, key, headers
