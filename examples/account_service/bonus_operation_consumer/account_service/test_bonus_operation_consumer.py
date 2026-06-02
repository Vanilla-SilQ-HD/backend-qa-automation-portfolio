import random
import time
from copy import deepcopy

import pytest
from hamcrest import assert_that, equal_to, none, not_none

import config
from account_service import service_name
from helpers import check_assert_that, data_generator
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_accounts_transactions import PortfolioAccountsTransactionsTopic
from kafka_topics.internal.portfolio_bonus_operation import PortfolioBonusOperationTopic

pytestmark = [
    allure.epic(service_name),
    allure.feature(config.bonus_operation_topic),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'Асинхронная обработка Portfolio.Bonus.Operation (bonusUnfrozen) в AccountService: '
        'разморозка бонусов (перенос из FrozenBalance в ActiveBalance), проводка по счёту '
        'и публикация сообщения в Portfolio.Accounts.Transactions.'
    ),
    allure.link('https://example.com/reward-ledger-demo-spec', name='Specification'),
    allure.severity('critical'),
    allure.tag(service_name),
    allure.tag('Kafka'),
    allure.tag('Portfolio.Bonus.Operation'),
]


class TestPositive:
    pytestmark = allure.story('Positive. Обработка Portfolio.Bonus.Operation (bonusUnfrozen)')

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
        with allure.step('Добавляем клиента с бонусным счётом и замороженным балансом в БД AccountService'):
            self.client_from_db_before = self.db.add_bonus_client(self.client, frozen_balance=self.amount)
            self.balance_before = deepcopy(self.client_from_db_before.one_account.balance)
            allure_attach(self.client_from_db_before, 'client_from_db_before')
            allure_attach(self.balance_before, 'balance_before')
            self.db.session.expunge_all()

    def _send_message(self):
        with allure.step('Генерируем сообщение'):
            topic = PortfolioBonusOperationTopic(
                client=self.client,
                event_type='bonusUnfrozen',
                amount=self.amount,
            )
            self.message, key, headers = topic.set_message()
            allure_attach(self.message, 'message')
        with allure.step(f'Кладем сообщение в топик {config.bonus_operation_topic}'):
            self.kafka.send_message(config.bonus_operation_topic, self.message, key=key, headers=headers)

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

    @allure.title('Проводка разморозки в БД AccountService')
    def test_bonus_unfreeze_transaction_created(self):
        with allure.step('Проверяем, что в BalanceTransactions появилась 1 запись'):
            transactions = self.client_from_db_after.one_account.balance_transactions
            check_assert_that(len(transactions), equal_to(1), 'Должна быть 1 BalanceTransaction')
            transaction = transactions[0]
            allure_attach(transaction, 'balance_transaction')
        with allure.step('Проверяем проводку разморозки бонусов'):
            check_assert_that(transaction.ClientId, equal_to(self.client.id), 'Ошибка в ClientId')
            check_assert_that(transaction.AccountId, equal_to(self.client.one_account.id),
                              'Ошибка в AccountId')
            check_assert_that(transaction.OperationId, none(), 'OperationId должен быть пустым')
            check_assert_that(transaction.Amount, equal_to(self.amount), 'Ошибка в Amount')
            check_assert_that(transaction.CurrencyId, equal_to(9991), 'Ошибка в CurrencyId')  # Бонусы
            check_assert_that(transaction.TypeId, equal_to(3), 'Ошибка в TypeId')  # Unfrozen — разморозка
            check_assert_that(transaction.EventId, equal_to(7772), 'Ошибка в EventId')  # BonusUnfrozen
            check_assert_that(transaction.StatusId, equal_to(1), 'Ошибка в StatusId')  # Completed
            check_assert_that(transaction.Description, equal_to('Разморозка бонусов'),
                              'Ошибка в Description')

    @allure.title('Перенос бонусов из FrozenBalance в ActiveBalance')
    def test_frozen_bonus_moved_to_active_balance(self):
        with allure.step('Проверяем баланс после разморозки'):
            balance_after = self.client_from_db_after.one_account.balance
            allure_attach(self.balance_before, 'balance_before')
            allure_attach(balance_after, 'balance_after')
            check_assert_that(balance_after.ActiveBalance,
                              equal_to(self.balance_before.ActiveBalance + self.amount),
                              'ActiveBalance должен увеличиться на сумму разморозки')
            check_assert_that(balance_after.FrozenBalance,
                              equal_to(self.balance_before.FrozenBalance - self.amount),
                              'FrozenBalance должен уменьшиться на сумму разморозки')
            check_assert_that(
                balance_after.is_equal(
                    self.balance_before, ignored=['ActiveBalance', 'FrozenBalance', 'ModifiedOn']),
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
                event_type='bonusUnfrozen',
            ).check_message(kafka_message)
