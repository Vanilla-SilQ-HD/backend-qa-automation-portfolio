from datetime import datetime, timezone
from typing import Optional

import pytest
from hamcrest import assert_that, equal_to, greater_than, matches_regexp, none, not_none
from pydantic import ConfigDict, Field

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
    bonusPackages: list[BonusPackage] = Field(default_factory=list)


class BonusOperationMessage(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    trace: BonusOperationTrace
    bonusOperation: BonusOperationBody


class PortfolioBonusOperationTopic(BaseTopic):
    expected_header_event_type_unfrozen = 'BonusUnfrozen'
    expected_header_event_type_accrual = 'bonusAccrualForBalance'
    expected_producer_name = 'Portfolio.BonusService.Api'

    def __init__(self):
        self.name = config.bonus_operation_topic

    @staticmethod
    def _decode_key(value):
        if isinstance(value, bytes):
            return value.decode()
        return value

    @staticmethod
    def _parse_iso_datetime(value: str) -> datetime:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))

    @staticmethod
    def _to_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _check_headers_unfrozen(self, message):
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
            equal_to(self.expected_header_event_type_unfrozen),
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

    def _check_headers_bonus_exchange(self, message):
        headers = self._get_headers_from_message(message)
        check_assert_that(
            headers.get('eventType'),
            equal_to('bonusExchange'),
            'В headers.eventType ожидается bonusExchange',
        )
        return headers

    def _check_headers_bonus_exchange_cancel(self, message):
        headers = self._get_headers_from_message(message)
        check_assert_that(
            headers.get('eventType'),
            equal_to('bonusExchangeCancel'),
            'В headers.eventType ожидается bonusExchangeCancel',
        )
        return headers

    def _check_bonus_unfrozen(self, message, bonus):
        check_assert_that(self._decode_key(message.key), not_none(), 'Ошибка в ключе')
        check_assert_that(self._decode_key(message.key), equal_to(str(bonus.ClientId)), 'Ошибка в ключе')

        headers = self._check_headers_unfrozen(message)
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
        return parsed

    def _check_bonus_accrual_for_balance(self, message, context: dict):
        check_assert_that(self._decode_key(message.key), equal_to(context['client_id']), 'Ошибка в ключе')

        headers = self._check_headers_accrual(message)
        allure_attach(headers, 'portfolio_bonus_operation_headers')

        parsed = BonusOperationMessage.model_validate(message.value)
        allure_attach(parsed.model_dump(mode='json'), 'portfolio_bonus_operation_parsed')

        check_assert_that(parsed.trace.notifyId, equal_to('bonusAccrualForBalance'), 'Ошибка в trace.notifyId')
        check_assert_that(parsed.trace.requestId, not_none(), 'trace.requestId должен быть заполнен')
        check_assert_that(parsed.trace.requestTime, not_none(), 'trace.requestTime должен быть заполнен')

        op = parsed.bonusOperation
        check_assert_that(op.clientId, equal_to(context['client_id']), 'Ошибка в bonusOperation.clientId')
        check_assert_that(op.accountId, equal_to(context['source_account_id']), 'Ошибка в bonusOperation.accountId')
        check_assert_that(op.eventType, equal_to(context['event_type_id']), 'Ошибка в bonusOperation.eventType')
        check_assert_that(op.operationId, none(), 'bonusOperation.operationId должен быть null')
        check_assert_that(op.accountType, none(), 'bonusOperation.accountType должен быть null')
        check_assert_that(
            len(op.bonusPackages),
            greater_than(0),
            'bonusPackages должен содержать минимум один пакет',
        )

        package = op.bonusPackages[0]

        if 'expected_bonus' in context:
            check_assert_that(op.amount, equal_to(context['expected_bonus']), 'Ошибка в bonusOperation.amount')
            check_assert_that(
                package.amountUsed,
                equal_to(context['expected_bonus']),
                'Ошибка в bonusPackages[0].amountUsed',
            )
        if 'balance_day' in context:
            check_assert_that(
                self._to_utc(self._parse_iso_datetime(op.eventTime)),
                equal_to(self._to_utc(self._parse_iso_datetime(context['balance_day']))),
                'Ошибка в bonusOperation.eventTime',
            )
        if 'bonus_row' in context:
            bonus_row = context['bonus_row']
            check_assert_that(package.bonusId, equal_to(bonus_row.Id), 'Ошибка в bonusPackages[0].bonusId')
            check_assert_that(
                self._to_utc(self._parse_iso_datetime(op.operationDate)),
                equal_to(self._to_utc(bonus_row.OperationDate)),
                'Ошибка в bonusOperation.operationDate',
            )
        if 'bonus_rule' in context:
            bonus_rule = context['bonus_rule']
            check_assert_that(package.currency, equal_to(bonus_rule.CurrencyId), 'Ошибка в bonusPackages[0].currency')
            check_assert_that(
                package.percentage,
                equal_to(float(bonus_rule.Percentage)),
                'Ошибка в bonusPackages[0].percentage',
            )
            check_assert_that(package.status, equal_to(bonus_rule.BonusStatusId), 'Ошибка в bonusPackages[0].status')

        return parsed

    def _check_bonus_exchange(self, message, context: dict):
        check_assert_that(
            self._decode_key(message.key),
            equal_to(str(context['client_id'])),
            'Ошибка в ключе (ожидается clientId)',
        )

        headers = self._check_headers_bonus_exchange(message)
        allure_attach(headers, 'portfolio_bonus_operation_exchange_headers')

        parsed = BonusOperationMessage.model_validate(message.value)
        allure_attach(parsed.model_dump(mode='json'), 'portfolio_bonus_operation_exchange_parsed')

        check_assert_that(parsed.trace.notifyId, equal_to('bonusExchange'), 'Ошибка в trace.notifyId')
        check_assert_that(parsed.trace.requestId, not_none(), 'trace.requestId должен быть заполнен')
        check_assert_that(parsed.trace.requestTime, not_none(), 'trace.requestTime должен быть заполнен')

        op = parsed.bonusOperation
        check_assert_that(op.clientId, equal_to(str(context['client_id'])), 'Ошибка в bonusOperation.clientId')
        check_assert_that(op.accountId, equal_to(str(context['account_id'])), 'Ошибка в bonusOperation.accountId')
        check_assert_that(op.operationId, equal_to(context['operation_id']), 'Ошибка в bonusOperation.operationId')
        check_assert_that(op.eventType, equal_to(context['event_type']), 'Ошибка в bonusOperation.eventType')
        check_assert_that(op.amount, equal_to(context['amount']), 'Ошибка в bonusOperation.amount')
        check_assert_that(len(op.bonusPackages), greater_than(0), 'bonusPackages должен быть заполнен')

        if 'bonus_packages' in context:
            expected_packages = context['bonus_packages']
            check_assert_that(
                len(op.bonusPackages),
                equal_to(len(expected_packages)),
                'Количество bonusPackages не совпадает с ожидаемым',
            )
            actual_by_bonus_id = {package.bonusId: package for package in op.bonusPackages}
            for expected_package in expected_packages:
                expected_bonus_id = expected_package['bonus_id']
                check_assert_that(
                    expected_bonus_id in actual_by_bonus_id,
                    equal_to(True),
                    f'Пакет bonusId={expected_bonus_id} отсутствует в сообщении',
                )
                package = actual_by_bonus_id[expected_bonus_id]
                check_assert_that(
                    package.amountUsed,
                    equal_to(expected_package['amount_used']),
                    f'Ошибка в amountUsed для bonusId={expected_bonus_id}',
                )
                check_assert_that(
                    package.currency,
                    equal_to(expected_package['currency']),
                    f'Ошибка в currency для bonusId={expected_bonus_id}',
                )
                check_assert_that(
                    package.percentage,
                    equal_to(float(expected_package['percentage'])),
                    f'Ошибка в percentage для bonusId={expected_bonus_id}',
                )
                check_assert_that(
                    package.status,
                    equal_to(expected_package['status']),
                    f'Ошибка в status для bonusId={expected_bonus_id}',
                )
            check_assert_that(
                sum(package.amountUsed for package in op.bonusPackages),
                equal_to(context['amount']),
                'Сумма amountUsed в bonusPackages должна совпадать с amount',
            )
        else:
            expected_package = context['bonus_package']
            package = op.bonusPackages[0]
            check_assert_that(package.bonusId, equal_to(expected_package['bonus_id']), 'Ошибка в bonusPackages[0].bonusId')
            check_assert_that(
                package.amountUsed,
                equal_to(expected_package['amount_used']),
                'Ошибка в bonusPackages[0].amountUsed',
            )
            check_assert_that(package.currency, equal_to(expected_package['currency']), 'Ошибка в bonusPackages[0].currency')
            check_assert_that(
                package.percentage,
                equal_to(float(expected_package['percentage'])),
                'Ошибка в bonusPackages[0].percentage',
            )
            check_assert_that(package.status, equal_to(expected_package['status']), 'Ошибка в bonusPackages[0].status')
        return parsed

    def _check_bonus_exchange_cancel(self, message, context: dict):
        check_assert_that(
            self._decode_key(message.key),
            equal_to(str(context['client_id'])),
            'Ошибка в ключе (ожидается clientId)',
        )

        headers = self._check_headers_bonus_exchange_cancel(message)
        allure_attach(headers, 'portfolio_bonus_operation_cancel_headers')

        parsed = BonusOperationMessage.model_validate(message.value)
        allure_attach(parsed.model_dump(mode='json'), 'portfolio_bonus_operation_cancel_parsed')

        check_assert_that(
            parsed.trace.notifyId,
            equal_to('bonusExchangeCancel'),
            'Ошибка в trace.notifyId',
        )

        op = parsed.bonusOperation
        check_assert_that(op.clientId, equal_to(str(context['client_id'])), 'Ошибка в bonusOperation.clientId')
        check_assert_that(op.accountId, equal_to(str(context['account_id'])), 'Ошибка в bonusOperation.accountId')
        check_assert_that(op.operationId, equal_to(context['operation_id']), 'Ошибка в bonusOperation.operationId')
        check_assert_that(op.eventType, not_none(), 'bonusOperation.eventType должен быть заполнен')
        check_assert_that(op.amount, equal_to(context['amount']), 'Ошибка в bonusOperation.amount')
        check_assert_that(op.eventTime, not_none(), 'bonusOperation.eventTime должен быть заполнен')
        check_assert_that(op.operationDate, not_none(), 'bonusOperation.operationDate должен быть заполнен')

        if op.bonusPackages and len(op.bonusPackages) > 0:
            package = op.bonusPackages[0]
            check_assert_that(package.bonusId, equal_to(context['bonus_id']), 'Ошибка в bonusPackages[0].bonusId')
            check_assert_that(package.status, equal_to(1), 'Ошибка в bonusPackages[0].status (ACTIVE)')
            check_assert_that(package.amountUsed, not_none(), 'Ошибка в bonusPackages[0].amountUsed')
            check_assert_that(
                package.currency,
                equal_to(context['expected_currency']),
                'Ошибка в bonusPackages[0].currency',
            )
        else:
            allure_attach([], 'portfolio_bonus_operation_cancel_bonusPackages_empty')

        return parsed

    def check_message(
        self,
        message,
        bonus=None,
        context: dict | None = None,
    ):
        assert_that(message, not_none(), 'Сообщение отсутствует')
        assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')

        if bonus is not None and context is not None:
            pytest.fail('Нужно передать только один режим проверки: bonus или context')
        if bonus is None and context is None:
            pytest.fail('Не передан режим проверки: ожидается bonus или context')

        if bonus is not None:
            return self._check_bonus_unfrozen(message, bonus)
        if context.get('expected_header_event_type') == 'bonusExchangeCancel':
            return self._check_bonus_exchange_cancel(message, context)
        if context.get('expected_header_event_type') == 'bonusExchange':
            return self._check_bonus_exchange(message, context)
        return self._check_bonus_accrual_for_balance(message, context=context)

    def set_message_bonus_exchange(
        self,
        *,
        client_id: str,
        account_id: str,
        operation_id: int,
        amount: int,
        bonus_packages: list[dict],
        event_time: str | None = None,
        operation_date: str | None = None,
        event_type_id: int = 4,
        request_id: str | None = None,
        request_time: str | None = None,
    ):
        from helpers import get_test_uuid

        rid = request_id or get_test_uuid().replace('-', '')
        rt = request_time or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
        et = event_time or rt
        od = operation_date or rt
        bonus_operation_body = {
            'clientId': str(client_id),
            'accountId': str(account_id),
            'operationId': operation_id,
            'eventType': event_type_id,
            'eventTime': et,
            'amount': amount,
            'operationDate': od,
            'bonusPackages': bonus_packages,
        }
        value = {
            'trace': {'notifyId': 'bonusExchange', 'requestId': rid, 'requestTime': rt},
            'bonusOperation': bonus_operation_body,
        }
        BonusOperationMessage.model_validate(value)
        headers = self._set_headers()
        headers.append(('eventType', 'bonusExchange'.encode()))
        key = str(client_id)
        return value, key, headers
