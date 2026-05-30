import random
import time

import pytest
from hamcrest import assert_that, equal_to, not_none

import config
from helpers import data_generator
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_operations_events import PortfolioOperationsEventsTopic
from kafka_topics.internal.portfolio_payments_result import PortfolioPaymentsResultTopic
from operation_service import service_name

pytestmark = [
    allure.epic(service_name),
    allure.feature(config.portfolio_payments_result_topic),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'Асинхронная обработка Portfolio.Payments.Result в OperationService: '
        'перевод операции в финальный статус и публикация Portfolio.Operations.Events.'
    ),
    allure.link('https://example.com/spec-redacted', name='Specification'),
    allure.severity('critical'),
    allure.tag(service_name),
    allure.tag('Kafka'),
    allure.tag('Portfolio.Payments.Result'),
]


class TestPositive:
    pytestmark = allure.story('Positive. Успешный результат PaymentGatewayAdapter (state=20)')

    @allure.title('Подготовка данных для тестов')
    @pytest.fixture(scope='class', autouse=True)
    def prepare(self, request, db, kafka):
        self = request.cls
        self.db = db
        self.kafka = kafka
        self._before(self)
        self._send_message(self)
        self._after(self)

    def _before(self):
        with allure.step('Генерируем клиента'):
            self.client = data_generator.Client()
            self.amount = random.randint(1000, 9000)
            allure_attach(self.client, 'client')
        with allure.step('Добавляем данные клиента в БД OperationService'):
            self.clients_accounts_data = self.db.add_clients_accounts_data(self.client)
            allure_attach(self.clients_accounts_data, 'clients_accounts_data')
        with allure.step('Создаём операцию в статусе Pending'):
            operation = data_generator.Operation(
                client_id=str(self.client.id),
                account_id=str(self.client.one_account.id),
                operation_type_name='P2PCredit',
                operation_state_name='Pending',
                amount=self.amount,
                currency=643,  # RUB
                fee=0,
                description='Пополнение кошелька',
            )
            self.operation_before = self.db.add_operation(operation)
            allure_attach(self.operation_before, 'operation_before')

    def _send_message(self):
        with allure.step('Генерируем сообщение'):
            topic = PortfolioPaymentsResultTopic(
                client_id=str(self.client.id),
                expected_operation_state_id=None,
                expected_auto_transfer_type=None,
            )
            self.message, key, headers = topic.set_message(
                operation_id=self.operation_before.Id,
                operation_state_id=20,  # Success
                operation_type_id=25,  # P2PCredit
                amount=self.amount,
                currency=643,  # RUB
                fee=0,
                sector_id=3013,
            )
            allure_attach(self.message, 'message')
        with allure.step(f'Кладем сообщение в топик {config.portfolio_payments_result_topic}'):
            self.kafka.send_message(config.portfolio_payments_result_topic, self.message, key=key, headers=headers)

    def _after(self):
        with allure.step('Ожидаем перевод операции в статус Success (20)'):
            self.operation_after = self.db.session.query(self.db.model.Operation) \
                .filter_by(Id=self.operation_before.Id).first()
            for _ in range(10):
                if self.operation_after.OperationStateId == 20:
                    break
                time.sleep(0.5)
                self.db.session.refresh(self.operation_after)
            allure_attach(self.operation_after, 'operation_after')

    @allure.title('Операция переведена в статус Success (20)')
    def test_operation_marked_success(self):
        with allure.step('Проверяем статус операции'):
            assert_that(self.operation_after.OperationStateId, equal_to(20),
                        'Операция должна перейти в статус Success (20)')

    @allure.title(f'Опубликовано сообщение в {config.portfolio_operations_events_topic}')
    def test_success_operation_event_published(self):
        with allure.step(f'Ожидаем сообщение в {config.portfolio_operations_events_topic}'):
            message = self.kafka.wait_message(
                topic_name=config.portfolio_operations_events_topic,
                search_criteria={'operationId': self.operation_before.Id, 'operationStateId': 20},
            )
            allure_attach(message, 'kafka_message')
            assert_that(message, not_none(), 'Сообщение в Kafka отсутствует')
        with allure.step(f'Проверяем сообщение в {config.portfolio_operations_events_topic}'):
            topic = PortfolioOperationsEventsTopic(operation=self.operation_after)
            topic._msisdn = self.clients_accounts_data.Msisdn
            topic.check_message(message)


class TestNegative:
    pytestmark = allure.story('Negative. Ошибка PaymentGatewayAdapter (state=25)')

    @allure.title('Подготовка данных для тестов')
    @pytest.fixture(scope='class', autouse=True)
    def prepare(self, request, db, kafka):
        self = request.cls
        self.db = db
        self.kafka = kafka
        self._before(self)
        self._send_message(self)
        self._after(self)

    def _before(self):
        with allure.step('Генерируем клиента'):
            self.client = data_generator.Client()
            self.amount = random.randint(1000, 9000)
            allure_attach(self.client, 'client')
        with allure.step('Добавляем данные клиента в БД OperationService'):
            self.clients_accounts_data = self.db.add_clients_accounts_data(self.client)
            allure_attach(self.clients_accounts_data, 'clients_accounts_data')
        with allure.step('Создаём операцию в статусе Pending'):
            operation = data_generator.Operation(
                client_id=str(self.client.id),
                account_id=str(self.client.one_account.id),
                operation_type_name='P2PCredit',
                operation_state_name='Pending',
                amount=self.amount,
                currency=643,  # RUB
                fee=0,
                description='Пополнение кошелька',
            )
            self.operation_before = self.db.add_operation(operation)
            allure_attach(self.operation_before, 'operation_before')

    def _send_message(self):
        with allure.step('Генерируем сообщение'):
            topic = PortfolioPaymentsResultTopic(
                client_id=str(self.client.id),
                expected_operation_state_id=None,
                expected_auto_transfer_type=None,
            )
            self.message, key, headers = topic.set_message(
                operation_id=self.operation_before.Id,
                operation_state_id=25,  # Error
                operation_type_id=25,  # P2PCredit
                amount=self.amount,
                currency=643,  # RUB
                reason_code_id=6,
                message='Insufficient funds',
            )
            allure_attach(self.message, 'message')
        with allure.step(f'Кладем сообщение в топик {config.portfolio_payments_result_topic}'):
            self.kafka.send_message(config.portfolio_payments_result_topic, self.message, key=key, headers=headers)

    def _after(self):
        with allure.step('Ожидаем перевод операции в статус Error (25)'):
            self.operation_after = self.db.session.query(self.db.model.Operation) \
                .filter_by(Id=self.operation_before.Id).first()
            for _ in range(10):
                if self.operation_after.OperationStateId == 25:
                    break
                time.sleep(0.5)
                self.db.session.refresh(self.operation_after)
            allure_attach(self.operation_after, 'operation_after')

    @allure.title('Операция переведена в статус Error (25)')
    def test_operation_marked_error(self):
        with allure.step('Проверяем статус операции и причину ошибки'):
            assert_that(self.operation_after.OperationStateId, equal_to(25),
                        'Операция должна перейти в статус Error (25)')
            assert_that(self.operation_after.ReasonCodeId, equal_to(6),
                        'ReasonCodeId должен сохраниться в Operations')
            assert_that(self.operation_after.Message, equal_to('Insufficient funds'),
                        'Message должен сохраниться в Operations')

    @allure.title(f'Опубликовано сообщение в {config.portfolio_operations_events_topic}')
    def test_error_operation_event_published(self):
        with allure.step(f'Ожидаем сообщение в {config.portfolio_operations_events_topic}'):
            message = self.kafka.wait_message(
                topic_name=config.portfolio_operations_events_topic,
                search_criteria={'operationId': self.operation_before.Id, 'operationStateId': 25},
            )
            allure_attach(message, 'kafka_message')
            assert_that(message, not_none(), 'Сообщение в Kafka отсутствует')
        with allure.step(f'Проверяем сообщение в {config.portfolio_operations_events_topic}'):
            topic = PortfolioOperationsEventsTopic(operation=self.operation_after)
            topic._msisdn = self.clients_accounts_data.Msisdn
            topic.check_message(message)
