from datetime import UTC, datetime
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
    Consumers: AccountService, BonusService, DemoPartnerAdapter
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
    reasonCodeId: Optional[int] = None
    message: Optional[str] = None
    amount: Optional[int] = None
    currency: Optional[int] = None
    fee: Optional[int] = None
    description: Optional[str] = None
    clientId: uuid_str
    precheckId: Optional[str] = None
    partnerOrderId: Optional[str] = None
    externalUserRef: Optional[str] = None
    accountId: Optional[uuid_str] = None
    accountIdTo: Optional[uuid_str] = None


def _utc_event_time_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class PortfolioOperationsEventsTopic(BaseTopic):
    """
    Producers: OperationService
    Consumers: AccountService, BonusService, DemoPartnerAdapter

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
                 description=None, external_user_ref=None, event_time=None,
                 reason_code_id=None, error_message=None):
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
        self._external_user_ref = external_user_ref
        self._event_time = event_time
        self._reason_code_id = reason_code_id
        self._error_message = error_message

    def set_message(self):
        """
        Формирует тело и заголовки исходящего сообщения Portfolio.Operations.Events (autotests producer).
        """
        event_time = self._event_time or _utc_event_time_iso()
        value: dict = {
            "operationId": self._operation_id,
            "operationStateId": self._operation_state_id,
            "operationStateName": self._operation_state_name,
            "operationTypeId": self._operation_type_id,
            "operationTypeName": self._operation_type_name,
            "eventTime": event_time,
            "amount": self._amount,
            "currency": self._currency,
            "fee": self._fee,
            "description": self._description,
            "clientId": str(self._client_id),
            "accountId": str(self._account_id) if self._account_id is not None else None,
            "externalUserRef": self._external_user_ref,
        }
        if self._reason_code_id is not None:
            value["reasonCodeId"] = self._reason_code_id
        if self._error_message is not None:
            value["message"] = self._error_message

        Message(**value)

        key = str(self._operation_id)
        headers = self._set_headers()
        headers.extend(
            [
                ("operationStateId", str(self._operation_state_id).encode()),
                ("operationId", str(self._operation_id).encode()),
                ("operationTypeId", str(self._operation_type_id).encode()),
            ]
        )
        return value, key, headers

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
        check_assert_that(str(msg.clientId), equal_to(str(self._client_id)), 'Ошибка в clientId')
        check_assert_that(msg.eventTime, not_none(), 'eventTime должен присутствовать')

        check_assert_that(str(msg.accountId), equal_to(str(self._account_id)), 'Ошибка в accountId')
        check_assert_that(msg.amount, equal_to(self._amount), 'Ошибка в amount')
        check_assert_that(msg.currency, equal_to(self._currency), 'Ошибка в currency')
        check_assert_that(msg.fee, equal_to(self._fee), 'Ошибка в fee')
        check_assert_that(msg.description, equal_to(self._description), 'Ошибка в description')
        check_assert_that(msg.externalUserRef, equal_to(self._external_user_ref), 'Ошибка в external_user_ref')
