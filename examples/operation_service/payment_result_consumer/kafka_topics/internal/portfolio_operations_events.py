from typing import Optional

from hamcrest import assert_that, not_none, equal_to
from kafka.consumer.fetcher import ConsumerRecord

import config
from helpers import check_assert_that
from kafka_topics.internal import BaseTopic
from helpers.pydantic_base_model import BasePydanticModel, uuid_str


# Portfolio.Operations.Events

class Message(BasePydanticModel):
    """
    Producers: OperationService
    Consumers: AccountService, BonusService, PaymentGatewayAdapter
    """
    operationId: int
    extId: Optional[str] = None
    operationStateId: int
    operationStateName: str
    operationTypeId: int
    operationTypeName: str
    partnerOperationId: Optional[str] = None
    partnerOperationDate: Optional[str] = None
    eventTime: str
    rrn: Optional[str] = None
    reasonCodeId: Optional[int] = None
    message: Optional[str] = None
    amount: Optional[int] = None
    currency: Optional[int] = None
    fee: Optional[int] = None
    description: Optional[str] = None
    clientId: uuid_str
    sbpBankId: Optional[str] = None
    precheckId: Optional[str] = None
    partnerOrderId: Optional[str] = None
    msisdn: Optional[str] = None
    phone: Optional[str] = None
    accountId: Optional[uuid_str] = None
    accountIdTo: Optional[uuid_str] = None


class PortfolioOperationsEventsTopic(BaseTopic):
    """
    Producers: OperationService
    Consumers: AccountService, BonusService, PaymentGatewayAdapter

    Status: bonus_exchange:
      10 - Created
      11 - CheckBalance
      12 - BonusOperation
      20 - Success
      25 - Error
    """

    def __init__(self, operation=None, operation_id=None, operation_state_id=None, operation_state_name=None,
                 operation_type_id=None, operation_type_name=None, client_id=None,
                 account_id=None, amount=None, currency=None, fee=None,
                 description=None, msisdn=None):
        self.name = config.portfolio_operations_events_topic
        if operation is not None:
            operation_id = operation.Id
            operation_state_id = operation.OperationStateId
            operation_state_name = operation.operation_state.Name
            operation_type_id = operation.OperationTypeId
            operation_type_name = operation.operation_type.Name
            client_id = operation.ClientId
            account_id = operation.AccountId
            amount = operation.Amount
            currency = operation.Currency
            fee = operation.Fee
            description = operation.Description

        self._operation_id = operation_id
        self._operation_state_id = operation_state_id
        self._operation_state_name = operation_state_name
        self._operation_type_id = operation_type_id
        self._operation_type_name = operation_type_name
        self._client_id = client_id
        self._account_id = account_id
        self._amount = amount
        self._currency = currency
        self._fee = fee
        self._description = description
        self._msisdn = msisdn

    def _check_key(self, message: ConsumerRecord):
        check_assert_that(message.key, not_none(), 'Ошибка в ключе')

    def _check_message_headers(self, message: ConsumerRecord):
        headers = self._get_headers_from_message(message)
        check_assert_that(
            headers.get('operationStateId'),
            equal_to(str(self._operation_state_id)),
            'Ошибка в operationStateId заголовка'
        )
        check_assert_that(
            headers.get('operationTypeId'),
            equal_to(str(self._operation_type_id)),
            'Ошибка в operationTypeId заголовка'
        )

    def check_message(self, message: ConsumerRecord):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')

        self._check_key(message)
        self._check_message_headers(message)

        msg = Message(**message.value)
        check_assert_that(msg.operationId, equal_to(self._operation_id), 'Ошибка в operationId')
        check_assert_that(msg.operationStateId, equal_to(self._operation_state_id),
                          'Ошибка в operationStateId')
        check_assert_that(msg.operationStateName, equal_to(self._operation_state_name),
                          'Ошибка в operationStateName')
        check_assert_that(msg.operationTypeId, equal_to(self._operation_type_id),
                          'Ошибка в operationTypeId')
        check_assert_that(msg.operationTypeName, equal_to(self._operation_type_name),
                          'Ошибка в operationTypeName')
        check_assert_that(msg.clientId, equal_to(self._client_id), 'Ошибка в clientId')
        check_assert_that(msg.eventTime, not_none(), 'eventTime должен присутствовать')

        check_assert_that(msg.accountId, equal_to(self._account_id), 'Ошибка в accountId')
        check_assert_that(msg.amount, equal_to(self._amount), 'Ошибка в amount')
        check_assert_that(msg.currency, equal_to(self._currency), 'Ошибка в currency')
        check_assert_that(msg.fee, equal_to(self._fee), 'Ошибка в fee')
        check_assert_that(msg.description, equal_to(self._description), 'Ошибка в description')
        check_assert_that(msg.msisdn, equal_to(self._msisdn), 'Ошибка в msisdn')
