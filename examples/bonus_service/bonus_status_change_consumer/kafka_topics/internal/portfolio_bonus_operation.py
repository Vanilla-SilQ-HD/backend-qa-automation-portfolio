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
    currency: int
    percentage: float
    status: int
    amountUsed: int


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
    expected_producer_name = 'Portfolio.BonusService.Api'

    def __init__(self):
        self.name = config.bonus_operation_topic

    def _check_key(self, message, bonus):
        check_assert_that(message.key, not_none(), 'Ошибка в ключе')
        check_assert_that(message.key, equal_to(str(bonus.ClientId)), 'Ошибка в ключе')

    def _check_headers(self, message):
        headers = self._get_headers_from_message(message)
        check_assert_that(
            headers.get('ProducerName'),
            equal_to(self.expected_producer_name),
            'Ошибка в ProducerName',
        )
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

    def check_message(
        self,
        message,
        bonus,
    ):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')
        self._check_key(message, bonus)

        headers = self._check_headers(message)
        allure_attach(headers, 'portfolio_bonus_operation_headers')

        parsed = BonusOperationMessage.model_validate(message.value)
        allure_attach(parsed.model_dump(mode='json'), 'portfolio_bonus_operation_parsed')

        check_assert_that(parsed.trace.notifyId, equal_to('BonusUnfrozen'), 'Ошибка в trace.notifyId')
        check_assert_that(parsed.trace.requestId, not_none(), 'trace.requestId должен быть заполнен')
        check_assert_that(parsed.trace.requestTime, not_none(), 'trace.requestTime должен быть заполнен')

        op = parsed.bonusOperation
        check_assert_that(op.clientId, equal_to(str(bonus.ClientId)), 'Ошибка в bonusOperation.clientId')
        check_assert_that(op.eventType, equal_to(2), 'Ошибка в bonusOperation.eventType')
        check_assert_that(op.amount, equal_to(bonus.CurrentBalance), 'Ошибка в bonusOperation.amount')
        check_assert_that(op.operationId, none(), 'bonusOperation.operationId должен быть null')
        check_assert_that(op.accountId, none(), 'bonusOperation.accountId должен быть null')
        check_assert_that(op.accountType, none(), 'bonusOperation.accountType должен быть null')
        check_assert_that(
            len(op.bonusPackages),
            greater_than(0),
            'bonusPackages должен содержать минимум один пакет',
        )

        package = op.bonusPackages[0]
        check_assert_that(package.bonusId, equal_to(bonus.Id), 'Ошибка в bonusPackages[0].bonusId')
        check_assert_that(package.status, equal_to(1), 'Ошибка в bonusPackages[0].status')
        check_assert_that(package.amountUsed, equal_to(bonus.CurrentBalance), 'Ошибка в bonusPackages[0].amountUsed')
        check_assert_that(package.currency, equal_to(bonus.rule.CurrencyId), 'Ошибка в bonusPackages[0].currency')
        check_assert_that(
            package.percentage,
            equal_to(bonus.rule.Percentage),
            'Ошибка в bonusPackages[0].percentage',
        )
