import time
from datetime import UTC, datetime

import pytest
from hamcrest import assert_that, equal_to, greater_than, not_none

from helpers import data_generator
from helpers.allure_client import allure, allure_attach
from helpers.data_generator import Operation as GenOperation
from kafka_topics.internal.portfolio_operations_cancel import PortfolioOperationsCancelPublisher
from operation_service import service_name


pytestmark = [
    allure.epic(service_name),
    allure.feature('Portfolio.Operations.Cancel -> OperationService'),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'OperationService обрабатывает Portfolio.Operations.Cancel: переводит операцию '
        'в ошибочный статус и сохраняет reasonCode/message из Kafka-сообщения.'
    ),
    allure.link('https://example.com/reward-ledger-demo-spec', name='Specification'),
    allure.severity('critical'),
    allure.tag(service_name),
    allure.tag('Kafka'),
]


class TestPositive:
    pytestmark = allure.story('Positive. Отмена RewardCreditBonus по Kafka-событию')

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
            self.operation_id = int(datetime.now(UTC).timestamp() * 1000) % 1_000_000_000_000 + 10_002

            clients_accounts_data = self.db.add_clients_accounts_data(self.client)
            allure_attach(clients_accounts_data, 'clients_accounts_data')

            operation = GenOperation(
                client_id=self.client_id,
                account_id=self.account_id,
                operation_type_name='RewardCreditBonus',
                operation_state_name='Created',
                amount=10_500,
                currency=9991,
                fee=0,
                description='Пополнение с бонусного счета',
            )
            self.operation_before = self.db.add_operation(operation)
            self.modified_before = self.operation_before.ModifiedOn
            allure_attach(self.operation_before, 'operation_before_cancel')

        with allure.step('Готовим данные cancel-события'):
            self.publisher = PortfolioOperationsCancelPublisher()
            self.reason_code_id = 60002
            self.error_message = 'Insufficient bonuses'
            self.cancel_message, self.key, self.headers = self.publisher.set_message(
                operation_id=self.operation_before.Id,
                operation_state_id=25,
                operation_state_name='Error',
                operation_type_id=26,
                operation_type_name=getattr(self.operation_before.operation_type, 'Name', None) or 'RewardCreditBonus',
                client_id=self.client_id,
                account_id=self.account_id,
                amount=operation.amount,
                currency=9991,
                reason_code_id=self.reason_code_id,
                message=self.error_message,
            )
            allure_attach(self.cancel_message, 'operation_cancel_message')

    def _send_message(self):
        with allure.step(f'Отправляем сообщение в {self.publisher.name}'):
            self.kafka.send_message(
                topic=self.publisher.name,
                message=self.cancel_message,
                key=self.key,
                headers=self.headers,
            )

    def _after(self):
        with allure.step('Ожидаем перевод операции в Error'):
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
                if self.operation_after is not None and self.operation_after.OperationStateId == 25:
                    break
                time.sleep(0.5)
            allure_attach(self.operation_after, 'operation_after_cancel')

    def _cleanup(self):
        with allure.step('Удаляем тестовую операцию'):
            try:
                model = self.db.model
                deleted = (
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
                allure_attach({'deleted_operations': deleted}, 'cleanup_operation')
            except Exception as exc:
                self.db.session.rollback()
                allure_attach({'error': repr(exc)}, 'cleanup_operation_error')

    @allure.title('Операция переведена в состояние Error')
    def test_operation_cancel_saved(self):
        assert_that(self.operation_after, not_none(), 'Операция не найдена в БД после ожидания')
        assert_that(self.operation_after.OperationStateId, equal_to(25), 'Ожидается OperationStateId=25')
        assert_that(self.operation_after.ModifiedOn, greater_than(self.modified_before), 'ModifiedOn должен обновиться')

    @allure.title('ReasonCode и текст ошибки сохранены из cancel-события')
    def test_operation_cancel_reason_saved(self):
        assert_that(self.operation_after, not_none(), 'Операция отсутствует в БД')
        assert_that(self.operation_after.ReasonCodeId, equal_to(self.reason_code_id), 'Некорректный ReasonCodeId')
        assert_that(self.operation_after.Message, equal_to(self.error_message), 'Некорректный Message')
