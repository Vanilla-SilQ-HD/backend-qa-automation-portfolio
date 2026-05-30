from typing import Optional

from hamcrest import assert_that, equal_to, not_none
from kafka.consumer.fetcher import ConsumerRecord

import config
from helpers import check_assert_that
from helpers.pydantic_base_model import BasePydanticModel, uuid_str
from kafka_topics.internal import BaseTopic


# Portfolio.Operations.Cancel

class Message(BasePydanticModel):
    """
    Producers: AccountService, BonusService
    Consumers: OperationService
    """
    operationId: int
    extId: Optional[str] = None
    operationStateId: int
    operationStateName: str
    operationTypeId: Optional[int] = None
    operationTypeName: Optional[str] = None
    eventTime: str
    reasonCodeId: Optional[int] = None
    message: Optional[str] = None
    amount: Optional[int] = None
    currency: Optional[int] = None
    fee: Optional[int] = None
    description: Optional[str] = None
    clientId: uuid_str
    msisdn: Optional[str] = None
    accountId: Optional[uuid_str] = None


class PortfolioOperationsCancelTopic(BaseTopic):
    """
    Producers: AccountService, BonusService
    Consumers: OperationService
    Topic: Portfolio.Operations.Cancel
    """

    def __init__(self, operation_id, operation_state_id, operation_state_name,
                 operation_type_id, client_id, account_id, amount, currency,
                 reason_code_id, error_message=None):
        self.name = config.portfolio_operations_cancel_topic
        self._operation_id = operation_id
        self._operation_state_id = operation_state_id
        self._operation_state_name = operation_state_name
        self._operation_type_id = operation_type_id
        self._client_id = client_id
        self._account_id = account_id
        self._amount = amount
        self._currency = currency
        self._reason_code_id = reason_code_id
        self._error_message = error_message

    def _check_key(self, message: ConsumerRecord):
        check_assert_that(message.key, not_none(), 'Ошибка в ключе')
        check_assert_that(message.key, equal_to(str(self._operation_id)), 'Ошибка в ключе')

    def check_message(self, message: ConsumerRecord):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')

        self._check_key(message)

        msg = Message(**message.value)
        check_assert_that(msg.operationId, equal_to(self._operation_id), 'Ошибка в operationId')
        check_assert_that(msg.operationStateId, equal_to(self._operation_state_id),
                          'Ошибка в operationStateId')
        check_assert_that(msg.operationStateName, equal_to(self._operation_state_name),
                          'Ошибка в operationStateName')
        check_assert_that(msg.operationTypeId, equal_to(self._operation_type_id),
                          'Ошибка в operationTypeId')
        check_assert_that(msg.clientId, equal_to(self._client_id), 'Ошибка в clientId')
        check_assert_that(msg.accountId, equal_to(self._account_id), 'Ошибка в accountId')
        check_assert_that(msg.amount, equal_to(self._amount), 'Ошибка в amount')
        check_assert_that(msg.currency, equal_to(self._currency), 'Ошибка в currency')
        check_assert_that(msg.reasonCodeId, equal_to(self._reason_code_id), 'Ошибка в reasonCodeId')
        check_assert_that(msg.eventTime, not_none(), 'eventTime должен присутствовать')
        if self._error_message is not None:
            check_assert_that(msg.message, equal_to(self._error_message), 'Ошибка в message')
        else:
            check_assert_that(msg.message, not_none(), 'message должен присутствовать')
