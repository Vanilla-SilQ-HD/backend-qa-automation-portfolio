import time

import pytest
from hamcrest import assert_that, equal_to, not_none
from sqlalchemy.orm import joinedload

import config
from helpers import data_generator
from helpers.allure_client import allure, allure_attach
from helpers.data_generator import Operation as GenOperation
from kafka_topics.internal.portfolio_bonus_operation import PortfolioBonusOperationTopic
from kafka_topics.internal.portfolio_operations_events import PortfolioOperationsEventsTopic
from operation_service import service_name


BONUS_CURRENCY_ID = 9991
EXPECTED_STATE_AFTER_BONUS = 12
STUB_BONUS_ID = 910595999


pytestmark = [
    allure.epic(service_name),
    allure.feature('Portfolio.Bonus.Operation -> OperationService'),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'OperationService обрабатывает Portfolio.Bonus.Operation: переводит операцию '
        'к следующему этапу платежного процесса и публикует Portfolio.Operations.Events.'
    ),
    allure.link('https://example.com/spec-redacted', name='Specification'),
    allure.severity('critical'),
    allure.tag(service_name),
    allure.tag('Kafka'),
]


class TestPositive:
    pytestmark = allure.story('Positive. Успешное списание бонусов двигает операцию дальше')

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
        with allure.step('Генерируем клиента и операцию в CheckBalance'):
            self.client = data_generator.Client()
            self.account_id = str(self.client.one_account.id)
            self.client_id = str(self.client.id)
            self.amount = 990

            clients_accounts_data = self.db.add_clients_accounts_data(self.client)
            allure_attach(clients_accounts_data, 'clients_accounts_data')

            operation = GenOperation(
                client_id=self.client_id,
                account_id=self.account_id,
                operation_type_name='P2PCreditBonus',
                operation_state_name='CheckBalance',
                amount=self.amount,
                currency=BONUS_CURRENCY_ID,
                fee=0,
                description='Пополнение с бонусного счета',
            )
            self.operation_before = self.db.add_operation(operation)
            allure_attach(self.operation_before, 'operation_before_bonus_operation')

        with allure.step('Готовим сообщение Portfolio.Bonus.Operation'):
            bonus_packages = [
                {
                    'bonusId': STUB_BONUS_ID,
                    'amountUsed': self.amount,
                    'currency': BONUS_CURRENCY_ID,
                    'percentage': 100.0,
                    'status': 1,
                }
            ]
            topic = PortfolioBonusOperationTopic()
            self.message, self.key, self.headers = topic.set_message_bonus_exchange(
                client_id=self.client_id,
                account_id=self.account_id,
                operation_id=self.operation_before.Id,
                amount=self.amount,
                bonus_packages=bonus_packages,
            )
            self.input_topic = topic.name
            allure_attach(self.message, 'bonus_operation_message')

    def _send_message(self):
        with allure.step(f'Отправляем сообщение в {self.input_topic}'):
            self.kafka.send_message(
                topic=self.input_topic,
                message=self.message,
                key=self.key,
                headers=self.headers,
            )

    def _after(self):
        with allure.step('Ожидаем переход операции в следующий статус'):
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
                    and self.operation_after.OperationStateId == EXPECTED_STATE_AFTER_BONUS
                ):
                    break
                time.sleep(0.5)
            allure_attach(self.operation_after, 'operation_after_bonus_operation')

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

    @allure.title('Операция перешла в статус после успешного списания бонусов')
    def test_operation_state_changed_after_bonus_operation(self):
        assert_that(self.operation_after, not_none(), 'Операция не найдена после ожидания')
        assert_that(
            self.operation_after.OperationStateId,
            equal_to(EXPECTED_STATE_AFTER_BONUS),
            'Ожидается переход в BonusOperation',
        )
        assert_that(self.operation_after.Amount, equal_to(self.amount), 'Сумма операции сохраняется')

    @allure.title('Опубликовано Portfolio.Operations.Events для платежного адаптера')
    def test_operations_event_for_payment_gateway_published(self):
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
                    'operationStateId': EXPECTED_STATE_AFTER_BONUS,
                },
                timeout=45,
                time_shift=300,
            )
            allure_attach(message, 'payment_gateway_operations_event')

        with allure.step('Проверяем сообщение через topic checker'):
            topic.check_message(message)
