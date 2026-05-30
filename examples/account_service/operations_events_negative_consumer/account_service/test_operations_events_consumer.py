import random
import time
from copy import deepcopy

import pytest
from hamcrest import assert_that, equal_to, not_none

import config
from account_service import service_name
from helpers import check_assert_that, data_generator
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_accounts_transactions import PortfolioAccountsTransactionsTopic
from kafka_topics.internal.portfolio_operations_events import PortfolioOperationsEventsTopic

pytestmark = [
    allure.epic(service_name),
    allure.feature(config.portfolio_operations_events_topic),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'Асинхронная обработка Portfolio.Operations.Events (state=25 Error) в AccountService: '
        'сторно-проводка по bonus-счёту, восстановление баланса '
        'и публикация сообщения в Portfolio.Accounts.Transactions.'
    ),
    allure.link('https://example.com/spec-redacted', name='Specification'),
    allure.severity('critical'),
    allure.tag(service_name),
    allure.tag('Kafka'),
    allure.tag('Portfolio.Operations.Events'),
]


class TestPositive:
    pytestmark = allure.story('Positive. Обработка Portfolio.Operations.Events state=25 (Error)')

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
        with allure.step('Добавляем клиента с бонусным счётом (balance=0) в БД AccountService'):
            self.client_from_db_before = self.db.add_bonus_client(self.client, balance_amount=0)
            self.balance_before = deepcopy(self.client_from_db_before.one_account.balance)
            allure_attach(self.client_from_db_before, 'client_from_db_before')
            allure_attach(self.balance_before, 'balance_before')
        with allure.step('Сидим положительную проводку BalanceTransaction в БД AccountService'):
            self.positive_transaction = self.db.add_balance_transaction(
                client_id=self.client.id,
                account_id=self.client.one_account.id,
                operation_id=int(time.time() * 1000),
                amount=self.amount,
                currency_id=9991,  # Бонусы
                type_id=2,  # Withdrawal — списание бонусов
                event_id=7774,  # BonusExchange
                description='Пополнение с бонусного счета',
            )
            self.positive_transaction_before = deepcopy(self.positive_transaction)
            allure_attach(self.positive_transaction_before, 'positive_transaction_before')
            self.db.session.expunge_all()

    def _send_message(self):
        with allure.step('Генерируем сообщение'):
            topic = PortfolioOperationsEventsTopic(
                client=self.client, amount=self.amount,
                operation_id=self.positive_transaction_before.OperationId,
                operation_state_id=25, operation_state_name='Error',
                reason_code_id=500, error_message='Rollback erroneous operation',
            )
            self.message, key, headers = topic.set_message()
            allure_attach(self.message, 'message')
        with allure.step(f'Кладем сообщение в топик {config.portfolio_operations_events_topic}'):
            self.kafka.send_message(config.portfolio_operations_events_topic, self.message, key=key, headers=headers)

    def _after(self):
        account_id = self.client.one_account.id
        with allure.step('Ожидаем сторно-проводку в BalanceTransactions'):
            for _ in range(20):
                self.db.session.expire_all()
                self.transactions_after = self.db.session.query(self.db.model.BalanceTransaction) \
                    .filter_by(AccountId=account_id) \
                    .order_by(self.db.model.BalanceTransaction.CreatedOn).all()
                if len(self.transactions_after) >= 2:
                    break
                time.sleep(0.5)
            allure_attach(self.transactions_after, 'transactions_after')
        with allure.step('Забираем баланс бонусного счёта после сторно'):
            self.balance_after = self.db.session.query(self.db.model.Balance) \
                .filter_by(AccountId=account_id).one_or_none()
            allure_attach(self.balance_after, 'balance_after')

    @allure.title('Сторно-проводка в БД AccountService')
    def test_storno_transaction_created(self):
        with allure.step('Проверяем, что в BalanceTransactions появились 2 записи (positive + storno)'):
            check_assert_that(len(self.transactions_after), equal_to(2),
                              'Должно быть 2 BalanceTransaction (positive + storno)')
            positive_after, storno_transaction = self.transactions_after
            allure_attach(positive_after, 'positive_transaction_after')
            allure_attach(storno_transaction, 'storno_transaction')
        with allure.step('Проверяем, что положительная проводка не изменилась'):
            check_assert_that(positive_after.is_equal(self.positive_transaction_before),
                              'Положительная проводка не должна меняться')
        with allure.step('Проверяем сторно-проводку'):
            check_assert_that(storno_transaction.OperationId,
                              equal_to(self.message['operationId']),
                              'Ошибка в OperationId')
            check_assert_that(storno_transaction.ClientId,
                              equal_to(self.message['clientId']),
                              'Ошибка в ClientId')
            check_assert_that(storno_transaction.AccountId,
                              equal_to(self.message['accountId']),
                              'Ошибка в AccountId')
            check_assert_that(storno_transaction.CurrencyId,
                              equal_to(self.message['currency']),
                              'Ошибка в CurrencyId')
            check_assert_that(storno_transaction.Amount,
                              equal_to(self.message['amount']),
                              'Ошибка в Amount')
            check_assert_that(storno_transaction.TypeId, equal_to(2), 'Ошибка в TypeId')  # Withdrawal
            check_assert_that(storno_transaction.EventId, equal_to(7775),
                              'Ошибка в EventId')  # BonusExchangeCancel
            check_assert_that(storno_transaction.StatusId, equal_to(2), 'Ошибка в StatusId')  # Cancelled
            check_assert_that(storno_transaction.Description, equal_to('Обмен бонусов. Отмена'),
                              'Ошибка в Description')

    @allure.title('Восстановление баланса после сторно')
    def test_bonus_balance_restored_after_storno(self):
        with allure.step('Проверяем баланс после сторно'):
            allure_attach(self.balance_before, 'balance_before')
            allure_attach(self.balance_after, 'balance_after')
            check_assert_that(self.balance_after.ActiveBalance, equal_to(self.amount),
                              'Активный баланс должен восстановиться до исходного значения')
            check_assert_that(self.balance_after.FrozenBalance, equal_to(0), 'Ошибка в FrozenBalance')
            check_assert_that(
                self.balance_after.is_equal(self.balance_before, ignored=['ActiveBalance', 'ModifiedOn']),
                'Некорректное изменение записи Balance',
            )

    @allure.title(f'Сообщение в {config.accounts_transactions_topic} после сторно')
    def test_storno_account_transaction_event_published(self):
        storno_transaction = self.transactions_after[-1]
        with allure.step(f'Ожидаем сторно-сообщение в {config.accounts_transactions_topic}'):
            storno_kafka_message = self.kafka.wait_message(
                topic_name=config.accounts_transactions_topic,
                search_criteria={'transactionId': storno_transaction.Id},
            )
            allure_attach(storno_kafka_message, 'storno_kafka_message')
            assert_that(storno_kafka_message, not_none(), 'Сторно-сообщение в Kafka отсутствует')
        with allure.step(f'Проверяем сторно-сообщение в {config.accounts_transactions_topic}'):
            PortfolioAccountsTransactionsTopic(
                transaction=storno_transaction,
                account_type_id=3,  # Бонусный
                event_type='bonusExchange',
            ).check_message(storno_kafka_message)
