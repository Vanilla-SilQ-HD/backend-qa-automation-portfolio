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
        'Асинхронная обработка Portfolio.Operations.Events (state=10 Created) в AccountService: '
        'проводка по bonus-счёту, списание баланса '
        'и публикация сообщения в Portfolio.Accounts.Transactions.'
    ),
    allure.link('https://example.com/reward-ledger-demo-spec', name='Specification'),
    allure.severity('critical'),
    allure.tag(service_name),
    allure.tag('Kafka'),
    allure.tag('Portfolio.Operations.Events'),
]


class TestPositive:
    pytestmark = allure.story('Positive. Обработка Portfolio.Operations.Events state=10 (Created)')

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
        with allure.step('Добавляем клиента с бонусным счётом в БД AccountService'):
            self.client_from_db_before = self.db.add_bonus_client(self.client, balance_amount=100_000)
            self.balance_before = deepcopy(self.client_from_db_before.one_account.balance)
            allure_attach(self.client_from_db_before, 'client_from_db_before')
            allure_attach(self.balance_before, 'balance_before')
            self.db.session.expunge_all()

    def _send_message(self):
        with allure.step('Генерируем сообщение'):
            topic = PortfolioOperationsEventsTopic(
                client=self.client, amount=self.amount,
                operation_state_id=10, operation_state_name='Created',
            )
            self.message, key, headers = topic.set_message()
            allure_attach(self.message, 'message')
        with allure.step(f'Кладем сообщение в топик {config.portfolio_operations_events_topic}'):
            self.kafka.send_message(config.portfolio_operations_events_topic, self.message, key=key, headers=headers)

    def _after(self):
        with allure.step('Ожидаем проводку BalanceTransaction в БД AccountService'):
            self.client_from_db_after = self.db.session.query(self.db.model.Client) \
                .filter_by(Id=self.client.id).first()
            for _ in range(20):
                if len(self.client_from_db_after.one_account.balance_transactions) >= 1:
                    break
                time.sleep(0.5)
                self.db.session.refresh(self.client_from_db_after)
            allure_attach(self.client_from_db_after, 'client_from_db_after')

    @allure.title('Проводка списания в БД AccountService')
    def test_withdrawal_transaction_created(self):
        with allure.step('Проверяем, что в BalanceTransactions появилась 1 запись'):
            transactions = self.client_from_db_after.one_account.balance_transactions
            check_assert_that(len(transactions), equal_to(1),
                              'Должна быть 1 BalanceTransaction')
            transaction = transactions[0]
            allure_attach(transaction, 'balance_transaction')
        with allure.step('Проверяем проводку списания'):
            check_assert_that(transaction.OperationId, equal_to(self.message['operationId']),
                              'Ошибка в OperationId')
            check_assert_that(transaction.ClientId, equal_to(self.message['clientId']),
                              'Ошибка в ClientId')
            check_assert_that(transaction.AccountId, equal_to(self.message['accountId']),
                              'Ошибка в AccountId')
            check_assert_that(transaction.CurrencyId, equal_to(self.message['currency']),
                              'Ошибка в CurrencyId')
            check_assert_that(transaction.Amount, equal_to(self.message['amount']),
                              'Ошибка в Amount')
            check_assert_that(transaction.TypeId, equal_to(2), 'Ошибка в TypeId')  # Withdrawal
            check_assert_that(transaction.EventId, equal_to(7774), 'Ошибка в EventId')  # BonusExchange
            check_assert_that(transaction.StatusId, equal_to(1), 'Ошибка в StatusId')  # Completed
            check_assert_that(transaction.Description, equal_to('Обмен бонусов'),
                              'Ошибка в Description')

    @allure.title('Списание суммы операции с баланса')
    def test_operation_amount_withdrawn_from_balance(self):
        with allure.step('Проверяем баланс после списания'):
            balance_after = self.client_from_db_after.one_account.balance
            allure_attach(self.balance_before, 'balance_before')
            allure_attach(balance_after, 'balance_after')
            check_assert_that(balance_after.ActiveBalance,
                              equal_to(self.balance_before.ActiveBalance - self.amount),
                              'Активный баланс должен уменьшиться на сумму операции')
            check_assert_that(balance_after.FrozenBalance, equal_to(0), 'Ошибка в FrozenBalance')
            check_assert_that(
                balance_after.is_equal(self.balance_before, ignored=['ActiveBalance', 'ModifiedOn']),
                'Некорректное изменение записи Balance',
            )

    @allure.title(f'Сообщение в {config.accounts_transactions_topic}')
    def test_account_transaction_event_published(self):
        transaction = self.client_from_db_after.one_account.balance_transactions[0]
        with allure.step(f'Ожидаем сообщение в {config.accounts_transactions_topic}'):
            kafka_message = self.kafka.wait_message(
                topic_name=config.accounts_transactions_topic,
                search_criteria={'transactionId': transaction.Id},
            )
            allure_attach(kafka_message, 'kafka_message')
            assert_that(kafka_message, not_none(), 'Сообщение в Kafka отсутствует')
        with allure.step(f'Проверяем сообщение в {config.accounts_transactions_topic}'):
            PortfolioAccountsTransactionsTopic(
                transaction=transaction,
                account_type_id=3,  # Бонусный
                event_type='bonusExchange',
            ).check_message(kafka_message)
