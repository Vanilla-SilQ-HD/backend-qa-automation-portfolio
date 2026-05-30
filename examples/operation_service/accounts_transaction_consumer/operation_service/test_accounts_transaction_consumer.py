import time
from datetime import UTC, datetime

import pytest
from hamcrest import assert_that, equal_to, not_none
from sqlalchemy.orm import joinedload

import config
from helpers import data_generator
from helpers.allure_client import allure, allure_attach
from helpers.data_generator import Operation as GenOperation
from kafka_topics.internal.portfolio_accounts_transactions import PortfolioAccountsTransactionsTopic
from kafka_topics.internal.portfolio_operations_events import PortfolioOperationsEventsTopic
from operation_service import service_name


BONUS_ACCOUNT_TYPE_ID = 3
BONUS_CURRENCY_ID = 9991
EXPECTED_STATE_AFTER_TRANSACTION = 11


pytestmark = [
    allure.epic(service_name),
    allure.feature('Portfolio.Accounts.Transactions -> OperationService'),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'OperationService обрабатывает успешную транзакцию AccountService: '
        'переводит бонусную операцию в CheckBalance и публикует Portfolio.Operations.Events.'
    ),
    allure.link('https://example.com/spec-redacted', name='Specification'),
    allure.severity('critical'),
    allure.tag(service_name),
    allure.tag('Kafka'),
]


class TestPositive:
    pytestmark = allure.story('Positive. bonusExchange completed обновляет операцию')

    @allure.title('Подготовка данных для тестов')
    @pytest.fixture(scope='class', autouse=True)
    def prepare(self, request, db, kafka):
        self = request.cls
        self.db = db
        self.kafka = kafka
        self._before(self)
        self._send_message(self)
        self._after(self)
        yield
        self._cleanup(self)

    def _before(self):
        with allure.step('Генерируем клиента и операцию в статусе Created'):
            self.client = data_generator.Client()
            self.account_id = str(self.client.one_account.id)
            self.client_id = str(self.client.id)
            self.amount = 1088

            clients_accounts_data = self.db.add_clients_accounts_data(self.client)
            allure_attach(clients_accounts_data, 'clients_accounts_data')

            operation = GenOperation(
                client_id=self.client_id,
                account_id=self.account_id,
                operation_type_name='P2PCreditBonus',
                operation_state_name='Created',
                amount=self.amount,
                currency=BONUS_CURRENCY_ID,
                fee=0,
                description='Пополнение с бонусного счета',
            )
            self.operation_before = self.db.add_operation(operation)
            allure_attach(self.operation_before, 'operation_before_accounts_transaction')

        with allure.step('Готовим сообщение Portfolio.Accounts.Transactions'):
            producer = PortfolioAccountsTransactionsTopic()
            self.event_time = datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
            self.message, self.key, self.headers = producer.set_message_bonus_exchange_completed(
                operation_id=self.operation_before.Id,
                account_id=self.account_id,
                client_id=self.client_id,
                amount=self.amount,
                currency=BONUS_CURRENCY_ID,
                account_type_id=BONUS_ACCOUNT_TYPE_ID,
                event_time=self.event_time,
            )
            self.input_topic = producer.name
            allure_attach(self.message, 'account_transaction_message')

    def _send_message(self):
        with allure.step(f'Отправляем сообщение в {self.input_topic}'):
            self.kafka.send_message(
                topic=self.input_topic,
                message=self.message,
                key=self.key,
                headers=self.headers,
            )

    def _after(self):
        with allure.step('Ожидаем переход операции в CheckBalance'):
            model = self.db.model
            deadline = time.time() + 45
            self.operation_after = None
            while time.time() < deadline:
                self.db.session.expire_all()
                self.operation_after = (
                    self.db.session.query(model.Operation)
                    .filter(model.Operation.Id == self.operation_before.Id)
                    .one_or_none()
                )
                if (
                    self.operation_after is not None
                    and self.operation_after.OperationStateId == EXPECTED_STATE_AFTER_TRANSACTION
                ):
                    break
                time.sleep(0.5)
            allure_attach(self.operation_after, 'operation_after_accounts_transaction')

    def _cleanup(self):
        with allure.step('Удаляем тестовую операцию'):
            try:
                model = self.db.model
                (
                    self.db.session.query(model.Operation)
                    .filter(model.Operation.Id == self.operation_before.Id)
                    .delete(synchronize_session=False)
                )
                (
                    self.db.session.query(model.ClientsAccountsData)
                    .filter(model.ClientsAccountsData.ClientId == self.client_id)
                    .delete(synchronize_session=False)
                )
                self.db.session.commit()
            except Exception as exc:
                self.db.session.rollback()
                allure_attach({'cleanup_error': repr(exc)}, 'cleanup')

    @allure.title('Операция перешла в CheckBalance после AccountService transaction')
    def test_operation_state_changed_after_account_transaction(self):
        assert_that(self.operation_after, not_none(), 'Операция не найдена после ожидания')
        assert_that(
            self.operation_after.OperationStateId,
            equal_to(EXPECTED_STATE_AFTER_TRANSACTION),
            'Ожидается переход в CheckBalance',
        )
        assert_that(self.operation_after.Amount, equal_to(self.amount), 'Сумма операции не должна теряться')
        assert_that(self.operation_after.Currency, equal_to(BONUS_CURRENCY_ID), 'Валюта операции не должна теряться')

    @allure.title('Опубликовано Portfolio.Operations.Events со статусом CheckBalance')
    def test_operations_event_state_11_published(self):
        model = self.db.model
        operation = (
            self.db.session.query(model.Operation)
            .options(joinedload(model.Operation.operation_state), joinedload(model.Operation.operation_type))
            .filter(model.Operation.Id == self.operation_before.Id)
            .one_or_none()
        )
        assert_that(operation, not_none(), 'Операция должна существовать перед проверкой Kafka')

        topic = PortfolioOperationsEventsTopic(operation=operation)
        with allure.step(f'Ожидаем сообщение в {config.portfolio_operations_events_topic}'):
            message = self.kafka.wait_message(
                topic_name=config.portfolio_operations_events_topic,
                search_criteria={
                    'operationId': self.operation_before.Id,
                    'operationStateId': EXPECTED_STATE_AFTER_TRANSACTION,
                },
                timeout=45,
                time_shift=300,
            )
            allure_attach(message, 'operations_event_message')

        with allure.step('Проверяем сообщение через topic checker'):
            topic.check_message(message)
