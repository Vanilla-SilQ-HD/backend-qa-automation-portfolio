from datetime import UTC, datetime
from uuid import uuid4

import pytest

from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_balance_end_of_day import PortfolioBalanceEndOfDayTopic

from e2e.bonus_accrual_assertions import BonusAccrualAssertions
from e2e.bonus_accrual_flow import (
    CLIENT_ID,
    SOURCE_ACCOUNT_TYPE_ID,
    BonusAccrualFlow,
)
from e2e.history_assertions import HistoryAssertions


pytestmark = [
    allure.epic('e2e'),
    allure.feature('Bonus accrual after end-of-day balance event'),
    allure.label('owner', 'portfolio'),
    allure.description(
        'End-to-end сценарий начисления бонусов: входящее событие BalanceEndOfDay, '
        'публикация Kafka-событий, записи в БД и отображение операции в истории клиента.'
    ),
    allure.severity('critical'),
    allure.tag('e2e'),
    allure.tag('Kafka'),
    allure.tag('Bonus'),
    allure.tag('History'),
    allure.tag('API'),
]


class TestBonusAccrualE2E:
    pytestmark = allure.story('E2E. Начисление бонусов после BalanceEndOfDay')

    @allure.title('Подготовка данных для тестов')
    @pytest.fixture(scope='class', autouse=True)
    def prepare(self, request, account_service_db, bonus_db, operation_db, kafka):
        self = request.cls
        self.account_db = account_service_db
        self.bonus_db = bonus_db
        self.operation_db = operation_db
        self.kafka = kafka
        self.flow = BonusAccrualFlow(self)
        self.assertions = BonusAccrualAssertions(self, self.flow)
        self.history_assertions = HistoryAssertions(self, self.flow)

        self.event_time = datetime.now(UTC).replace(microsecond=0).isoformat()
        self.source_account_id = str(uuid4())
        self.balance_amount = 5_000_000
        self.tx_id = None
        self.amount = None
        self.bonus_message = None
        self.tx_message = None

        self.balance_end_of_day_topic = PortfolioBalanceEndOfDayTopic(
            source_account_id=self.source_account_id,
            client_id=CLIENT_ID,
            account_type=SOURCE_ACCOUNT_TYPE_ID,
            event_time=self.event_time,
            balance=self.balance_amount,
        )

        self._before(self)
        self._send_message(self)
        self._after(self)
        yield
        self._cleanup(self)

    def _before(self):
        with allure.step('Готовим данные для BalanceEndOfDay e2e'):
            self.flow.prepare_source_account()

    def _send_message(self):
        message, key, headers = self.balance_end_of_day_topic.set_message()
        with allure.step(f'Отправляем событие в {self.balance_end_of_day_topic.name}'):
            allure_attach(message, 'balance_end_of_day_message')
            self.kafka.send_message(
                topic=self.balance_end_of_day_topic.name,
                message=message,
                key=key,
                headers=headers,
            )

    def _after(self):
        self.account_db.session.expire_all()
        self.bonus_db.session.expire_all()

    def _cleanup(self):
        self.flow.cleanup()

    @allure.title('Событие начисления опубликовано в Portfolio.Bonus.Operation')
    def test_bonus_operation_event_published(self):
        self.assertions.assert_bonus_operation_event()

    @allure.title('Бонусная запись создана с рассчитанной суммой начисления')
    def test_bonus_accrual_row_created(self):
        self.assertions.assert_bonus_row()

    @allure.title('Событие транзакции опубликовано в Portfolio.Account.Transactions')
    def test_account_transaction_event_published(self):
        self.assertions.assert_transaction_event()

    @allure.title('Транзакция начисления сохранена в BalanceTransactions')
    def test_balance_transaction_row_created(self):
        self.assertions.assert_transaction_row()

    @allure.title('Бонусный баланс обновлён после начисления')
    def test_bonus_balance_updated_after_accrual(self):
        self.assertions.assert_bonus_balance()

    @allure.title('Начисление отображается в истории клиента')
    def test_bonus_accrual_visible_in_client_history(self):
        self.history_assertions.assert_history()
