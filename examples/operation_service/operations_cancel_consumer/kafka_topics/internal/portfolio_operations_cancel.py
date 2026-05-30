from typing import Optional

from hamcrest import assert_that, equal_to, not_none
from pydantic import ConfigDict

import config
from helpers import check_assert_that
from helpers.allure_client import allure_attach
from helpers.pydantic_base_model import BasePydanticModel, uuid_str
from kafka_topics.internal import BaseTopic


class PortfolioOperationsCancelMessage(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    operationId: int
    operationStateId: int
    operationStateName: str
    operationTypeId: Optional[int] = None
    operationTypeName: Optional[str] = None
    eventTime: str
    amount: Optional[int] = None
    currency: Optional[int] = None
    clientId: uuid_str
    accountId: Optional[uuid_str] = None
    reasonCodeId: Optional[int] = None
    message: Optional[str] = None


class PortfolioOperationsCancelTopic(BaseTopic):
    def __init__(self):
        self.name = config.portfolio_operations_cancel_topic

    @staticmethod
    def _decode_key(value):
        if isinstance(value, bytes):
            return value.decode()
        return value

    def check_message(self, message, context: dict):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')

        headers = self._get_headers_from_message(message)
        allure_attach(headers, 'portfolio_operations_cancel_headers')

        check_assert_that(
            self._decode_key(message.key),
            equal_to(str(context['operation_id'])),
            'Ошибка в ключе (ожидается operationId)',
        )

        parsed = PortfolioOperationsCancelMessage.model_validate(message.value)
        allure_attach(parsed.model_dump(mode='json'), 'portfolio_operations_cancel_parsed')

        check_assert_that(parsed.operationId, equal_to(context['operation_id']), 'Ошибка в operationId')
        check_assert_that(parsed.operationStateId, equal_to(context['operation_state_id']), 'Ошибка в operationStateId')
        check_assert_that(
            parsed.operationStateName,
            equal_to(context['operation_state_name']),
            'Ошибка в operationStateName',
        )
        check_assert_that(parsed.operationTypeId, equal_to(context['operation_type_id']), 'Ошибка в operationTypeId')
        check_assert_that(str(parsed.clientId), equal_to(str(context['client_id'])), 'Ошибка в clientId')
        check_assert_that(str(parsed.accountId), equal_to(str(context['account_id'])), 'Ошибка в accountId')
        check_assert_that(parsed.amount, equal_to(context['amount']), 'Ошибка в amount')
        check_assert_that(parsed.currency, equal_to(context['currency']), 'Ошибка в currency')
        check_assert_that(parsed.reasonCodeId, equal_to(context['reason_code_id']), 'Ошибка в reasonCodeId')

        expected_message = context.get('expected_message')
        if expected_message:
            check_assert_that(parsed.message, equal_to(expected_message), 'Ошибка в message')
        else:
            check_assert_that(parsed.message, not_none(), 'message должен быть заполнен')


class PortfolioOperationsCancelPublisher(BaseTopic):
    """Формирование сообщения Portfolio.Operations.Cancel для автотестов."""

    def __init__(self):
        self.name = config.portfolio_operations_cancel_topic

    def set_message(
        self,
        *,
        operation_id: int,
        operation_state_id: int,
        operation_state_name: str,
        operation_type_id: int | None,
        operation_type_name: str | None,
        client_id: str,
        account_id: str | None,
        amount: int | None,
        currency: int | None,
        reason_code_id: int | None,
        message: str | None,
        event_time: str | None = None,
    ):
        from datetime import UTC, datetime

        et = event_time or datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
        value = {
            'operationId': operation_id,
            'operationStateId': operation_state_id,
            'operationStateName': operation_state_name,
            'operationTypeId': operation_type_id,
            'operationTypeName': operation_type_name,
            'eventTime': et,
            'amount': amount,
            'currency': currency,
            'clientId': client_id,
            'accountId': account_id,
            'reasonCodeId': reason_code_id,
            'message': message,
        }
        PortfolioOperationsCancelMessage.model_validate(value)
        key = str(operation_id)
        headers = self._set_headers()
        headers.extend(
            [
                ('operationStateId', str(operation_state_id).encode()),
                ('operationId', str(operation_id).encode()),
            ]
        )
        if operation_type_id is not None:
            headers.append(('operationTypeId', str(operation_type_id).encode()))
        return value, key, headers
