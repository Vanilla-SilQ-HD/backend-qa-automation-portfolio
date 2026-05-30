from datetime import datetime, timedelta, timezone

from hamcrest import not_none, equal_to
from kafka.consumer.fetcher import ConsumerRecord
from pydantic import BaseModel, ConfigDict

import config
from helpers import check_assert_that, datetime_now
from kafka_topics.internal import BaseTopic
from helpers.pydantic_base_model import uuid_str


class PortfolioPaymentsResultMessage(BaseModel):
    """Сообщение топика Portfolio.Payments.Result (автосписание с накопа — поле autoTransferType)."""

    model_config = ConfigDict(extra='ignore', strict=True, validate_assignment=True)

    operationStateId: int
    eventTime: str
    sectorId: int
    partnerOperationDate: str
    reasonCodeId: int
    maskedPan: str
    amount: int
    fee: int
    currency: int
    terminalId: str
    clientId: uuid_str
    autoTransferType: int


class PortfolioPaymentsResultTopic(BaseTopic):
    """
    ProducerName в заголовке Kafka: Portfolio.PaymentGatewayAdapter.Api
    Topic: Portfolio.Payments.Result
    """

    def __init__(
        self,
        *,
        client_id: str,
        expected_operation_state_id: int,
        expected_auto_transfer_type: int,
    ):
        self.name = config.portfolio_payments_result_topic
        self._client_id = client_id
        self._operation_state_id = expected_operation_state_id
        self._auto_transfer_type = expected_auto_transfer_type

    def search_criteria(self) -> dict:
        return {'clientId': self._client_id}

    def _check_key(self, message: ConsumerRecord):
        check_assert_that(message.key, not_none(), 'Ошибка в ключе')
        check_assert_that(message.key, equal_to(self._client_id), 'Ошибка в ключе')

    def check_message(self, message: ConsumerRecord):
        check_assert_that(message, not_none(), 'Сообщение отсутствует')
        check_assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')

        self._check_key(message)
        self._check_headers(message, producer_name='Portfolio.PaymentGatewayAdapter.Api')

        val = message.value
        parsed = PortfolioPaymentsResultMessage(**val)

        check_assert_that(parsed.operationStateId, equal_to(self._operation_state_id), 'Ошибка в operationStateId')
        check_assert_that(parsed.eventTime, not_none(), 'Ошибка в eventTime')
        check_assert_that(parsed.sectorId, not_none(), 'Ошибка в sectorId')
        check_assert_that(parsed.partnerOperationDate, not_none(), 'Ошибка в partnerOperationDate')
        check_assert_that(parsed.reasonCodeId, not_none(), 'Ошибка в reasonCodeId')
        check_assert_that(parsed.maskedPan, not_none(), 'Ошибка в maskedPan')
        check_assert_that(parsed.amount, not_none(), 'Ошибка в amount')
        check_assert_that(parsed.fee, not_none(), 'Ошибка в fee')
        check_assert_that(parsed.currency, not_none(), 'Ошибка в currency')
        check_assert_that(parsed.terminalId, not_none(), 'Ошибка в terminalId')
        check_assert_that(parsed.clientId, equal_to(self._client_id), 'Ошибка в clientId')
        check_assert_that(parsed.autoTransferType, equal_to(self._auto_transfer_type), 'Ошибка в autoTransferType')

        event_time = datetime.fromisoformat(parsed.eventTime.replace('Z', '+00:00'))
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)
        now = datetime_now(with_offset_naive=False)
        check_assert_that(abs(event_time - now) < timedelta(minutes=5), 'Ошибка в eventTime')

    def set_message(self, *, operation_id, operation_state_id, operation_type_id,
                    amount=None, currency=None, fee=None, sector_id=None,
                    reason_code_id=None, message=None, event_time=None):
        event_time = event_time or datetime_now().strftime('%Y-%m-%dT%H:%M:%SZ')
        value = {
            'operationId': operation_id,
            'clientId': str(self._client_id),
            'operationStateId': operation_state_id,
            'operationTypeId': operation_type_id,
            'eventTime': event_time,
        }
        if amount is not None:
            value['amount'] = amount
        if currency is not None:
            value['currency'] = currency
        if fee is not None:
            value['fee'] = fee
        if sector_id is not None:
            value['sectorId'] = sector_id
        if reason_code_id is not None:
            value['reasonCodeId'] = reason_code_id
        if message is not None:
            value['message'] = message
        key = str(self._client_id)
        headers = self._set_headers()
        headers.extend([
            ('operationStateId', str(operation_state_id).encode()),
            ('operationId', str(operation_id).encode()),
        ])
        return value, key, headers
