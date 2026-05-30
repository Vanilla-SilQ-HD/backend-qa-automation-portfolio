import random
import time
from copy import deepcopy
from datetime import timedelta

import pytest
from hamcrest import assert_that, equal_to, none, not_none

import config
from bonus_service import service_name
from helpers import check_assert_that, datetime_now, get_test_uuid
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_bonus_operation import PortfolioBonusOperationTopic
from kafka_topics.internal.portfolio_operations_cancel import PortfolioOperationsCancelTopic
from kafka_topics.internal.portfolio_operations_events import PortfolioOperationsEventsTopic

pytestmark = [
    allure.epic(service_name),
    allure.feature(config.portfolio_operations_events_topic),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'Асинхронная обработка Portfolio.Operations.Events (state=11 CheckBalance) в BonusService: '
        'списание бонусных пакетов при достаточном балансе и отмена операции при нехватке бонусов.'
    ),
    allure.link('https://example.com/spec-redacted', name='Specification'),
    allure.severity('critical'),
    allure.tag(service_name),
    allure.tag('Kafka'),
    allure.tag('Portfolio.Operations.Events'),
]


class TestPositive:
    pytestmark = allure.story('Positive. Обработка Portfolio.Operations.Events state=11 (бонусов хватает)')

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
        with allure.step('Генерируем данные операции'):
            self.client_id = get_test_uuid()
            self.account_id = get_test_uuid()
            self.operation_id = random.randint(1_000_000_000, 2_100_000_000)
            self.amount = random.randint(1000, 9000)
            self.msisdn = random.choice(config.contract_msisdns)
        with allure.step('Получаем действующее правило начисления бонусов'):
            self.accrual_rule = self.db.get_active_accrual_rule(9991)  # Бонусы
            assert_that(self.accrual_rule, not_none(), 'Не найден действующий AccrualRule')
            allure_attach(self.accrual_rule, 'accrual_rule')
        with allure.step('Добавляем два активных бонусных пакета в БД BonusService'):
            now = datetime_now()
            first_amount = self.amount // 2
            self.bonus_1 = self.db.add_bonus(
                client_id=self.client_id, account_id=self.account_id,
                rule_id=self.accrual_rule.RuleId, amount=first_amount,
                expired_date=now + timedelta(days=10),
            )
            self.bonus_2 = self.db.add_bonus(
                client_id=self.client_id, account_id=self.account_id,
                rule_id=self.accrual_rule.RuleId, amount=self.amount - first_amount,
                expired_date=now + timedelta(days=20),
            )
            self.bonus_ids = [self.bonus_1.Id, self.bonus_2.Id]
            allure_attach([self.bonus_1, self.bonus_2], 'bonuses_before')
            self.db.session.expunge_all()

    def _send_message(self):
        with allure.step('Генерируем сообщение'):
            topic = PortfolioOperationsEventsTopic(
                operation_id=self.operation_id,
                operation_state_id=11, operation_state_name='CheckBalance',
                operation_type_id=26, operation_type_name='P2PCreditBonus',
                client_id=self.client_id, account_id=self.account_id,
                amount=self.amount, currency=9991,
                description='Пополнение с бонусного счета', msisdn=self.msisdn,
            )
            self.message, key, headers = topic.set_message()
            allure_attach(self.message, 'message')
        with allure.step(f'Кладем сообщение в топик {config.portfolio_operations_events_topic}'):
            self.kafka.send_message(config.portfolio_operations_events_topic, self.message, key=key, headers=headers)

    def _after(self):
        m = self.db.model
        with allure.step('Ожидаем WithdrawallOperation и детали списания в БД BonusService'):
            self.withdrawall_operation = None
            self.withdrawall_details = []
            for _ in range(90):
                self.db.session.expire_all()
                operation = self.db.session.query(m.WithdrawallOperation)\
                    .filter(m.WithdrawallOperation.OperationId == self.operation_id).one_or_none()
                if operation is not None:
                    details = self.db.session.query(m.WithdrawallOperationDetail)\
                        .filter(m.WithdrawallOperationDetail.WithdrawallOperationId == operation.Id).all()
                    if details:
                        self.withdrawall_operation = operation
                        self.withdrawall_details = details
                        break
                time.sleep(0.5)
            allure_attach(self.withdrawall_operation, 'withdrawall_operation')
            allure_attach(self.withdrawall_details, 'withdrawall_details')
        with allure.step('Перечитываем бонусные пакеты после обработки'):
            self.bonuses_after = self.db.session.query(m.Bonuses)\
                .filter(m.Bonuses.Id.in_(self.bonus_ids)).order_by(m.Bonuses.ExpiredDate).all()
            allure_attach(self.bonuses_after, 'bonuses_after')

    @allure.title('Создана WithdrawallOperation со списанием бонусов')
    def test_withdrawal_operation_created(self):
        with allure.step('Проверяем WithdrawallOperation'):
            check_assert_that(self.withdrawall_operation, not_none(), 'WithdrawallOperation не создана')
            check_assert_that(self.withdrawall_operation.OperationId, equal_to(self.operation_id),
                              'Ошибка в OperationId')
            check_assert_that(self.withdrawall_operation.ClientId, equal_to(self.client_id),
                              'Ошибка в ClientId')
            check_assert_that(self.withdrawall_operation.Amount, equal_to(self.amount), 'Ошибка в Amount')
            check_assert_that(self.withdrawall_operation.WithdrawalStatusId, equal_to(1),
                              'Ошибка в WithdrawalStatusId')  # Completed
        with allure.step('Проверяем детали списания'):
            check_assert_that(len(self.withdrawall_details), equal_to(len(self.bonus_ids)),
                              'Количество деталей должно совпадать с количеством бонусных пакетов')
            check_assert_that(sum(detail.AmountUsed for detail in self.withdrawall_details),
                              equal_to(self.amount), 'Сумма AmountUsed должна совпадать с amount операции')
            check_assert_that({detail.BonusId for detail in self.withdrawall_details},
                              equal_to(set(self.bonus_ids)),
                              'Детали должны быть созданы для всех подготовленных бонусов')

    @allure.title('Бонусные пакеты полностью списаны')
    def test_bonus_packages_written_off(self):
        with allure.step('Проверяем списание бонусов'):
            check_assert_that(len(self.bonuses_after), equal_to(len(self.bonus_ids)),
                              'Должны быть найдены все подготовленные бонусы')
            check_assert_that(sum(bonus.CurrentBalance for bonus in self.bonuses_after), equal_to(0),
                              'Суммарный CurrentBalance должен быть равен 0 после полного списания')
            for bonus in self.bonuses_after:
                check_assert_that(bonus.StatusId, equal_to(3), 'Ошибка в StatusId')  # USED

    @allure.title(f'Опубликовано сообщение в {config.bonus_operation_topic}')
    def test_bonus_operation_event_published(self):
        bonuses_by_id = {bonus.Id: bonus for bonus in self.bonuses_after}
        bonus_packages = [
            {
                'bonus_id': detail.BonusId,
                'amount_used': detail.AmountUsed,
                'currency': self.accrual_rule.CurrencyId,
                'percentage': self.accrual_rule.Percentage,
                'status': bonuses_by_id[detail.BonusId].StatusId,
            }
            for detail in self.withdrawall_details
        ]
        with allure.step(f'Ожидаем сообщение в {config.bonus_operation_topic}'):
            kafka_message = self.kafka.wait_message(
                topic_name=config.bonus_operation_topic,
                search_criteria={'bonusOperation': {'operationId': self.operation_id}},
            )
            allure_attach(kafka_message, 'kafka_message')
            assert_that(kafka_message, not_none(), 'Сообщение в Kafka отсутствует')
        with allure.step(f'Проверяем сообщение в {config.bonus_operation_topic}'):
            PortfolioBonusOperationTopic(
                client_id=self.client_id,
                account_id=self.account_id,
                operation_id=self.operation_id,
                amount=self.amount,
                event_type=4,  # bonusExchange
                bonus_packages=bonus_packages,
            ).check_message(kafka_message)


class TestNegative:
    pytestmark = allure.story('Negative. Обработка Portfolio.Operations.Events state=11 (бонусов недостаточно)')

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
        with allure.step('Генерируем данные операции'):
            self.client_id = get_test_uuid()
            self.account_id = get_test_uuid()
            self.operation_id = random.randint(2_100_000_001, 2_500_000_000)
            self.amount = random.randint(4000, 9000)
            self.msisdn = random.choice(config.contract_msisdns)
        with allure.step('Получаем действующее правило начисления бонусов'):
            self.accrual_rule = self.db.get_active_accrual_rule(9991)  # Бонусы
            assert_that(self.accrual_rule, not_none(), 'Не найден действующий AccrualRule')
        with allure.step('Добавляем бонусный пакет с недостаточным балансом в БД BonusService'):
            self.bonus = self.db.add_bonus(
                client_id=self.client_id, account_id=self.account_id,
                rule_id=self.accrual_rule.RuleId, amount=self.amount - 1000,
                expired_date=datetime_now() + timedelta(days=30),
            )
            self.bonus_id = self.bonus.Id
            self.bonus_before = deepcopy(self.bonus)
            allure_attach(self.bonus_before, 'bonus_before')
            self.db.session.expunge_all()

    def _send_message(self):
        with allure.step('Генерируем сообщение'):
            topic = PortfolioOperationsEventsTopic(
                operation_id=self.operation_id,
                operation_state_id=11, operation_state_name='CheckBalance',
                operation_type_id=26, operation_type_name='P2PCreditBonus',
                client_id=self.client_id, account_id=self.account_id,
                amount=self.amount, currency=9991,
                description='Пополнение с бонусного счета', msisdn=self.msisdn,
            )
            self.message, key, headers = topic.set_message()
            allure_attach(self.message, 'message')
        with allure.step(f'Кладем сообщение в топик {config.portfolio_operations_events_topic}'):
            self.kafka.send_message(config.portfolio_operations_events_topic, self.message, key=key, headers=headers)

    def _after(self):
        m = self.db.model
        with allure.step(f'Ожидаем сообщение в {config.portfolio_operations_cancel_topic}'):
            self.cancel_message = self.kafka.wait_message(
                topic_name=config.portfolio_operations_cancel_topic,
                search_criteria={'operationId': self.operation_id},
            )
            allure_attach(self.cancel_message, 'cancel_message')
        with allure.step('Читаем состояние БД BonusService после обработки'):
            self.db.session.expire_all()
            self.withdrawall_operation = self.db.session.query(m.WithdrawallOperation)\
                .filter(m.WithdrawallOperation.OperationId == self.operation_id).one_or_none()
            self.bonus_after = self.db.session.query(m.Bonuses)\
                .filter(m.Bonuses.Id == self.bonus_id).one()
            allure_attach(self.withdrawall_operation, 'withdrawall_operation')
            allure_attach(self.bonus_after, 'bonus_after')

    @allure.title('WithdrawallOperation не создана, бонусный пакет не изменён')
    def test_withdrawal_not_created_when_bonus_is_insufficient(self):
        with allure.step('Проверяем отсутствие WithdrawallOperation'):
            check_assert_that(self.withdrawall_operation, none(),
                              'При нехватке бонусов WithdrawallOperation не должна создаваться')
        with allure.step('Проверяем, что бонусный пакет не изменился'):
            check_assert_that(self.bonus_after.CurrentBalance, equal_to(self.bonus_before.CurrentBalance),
                              'CurrentBalance не должен изменяться')
            check_assert_that(self.bonus_after.StatusId, equal_to(self.bonus_before.StatusId),
                              'StatusId не должен изменяться')
            check_assert_that(
                self.bonus_after.is_equal(self.bonus_before, ignored=['ModifiedOn']),
                'Бонусный пакет не должен изменяться',
            )

    @allure.title(f'Опубликовано сообщение в {config.portfolio_operations_cancel_topic}')
    def test_operation_cancel_event_published(self):
        with allure.step(f'Проверяем сообщение в {config.portfolio_operations_cancel_topic}'):
            assert_that(self.cancel_message, not_none(), 'Сообщение в Kafka отсутствует')
            PortfolioOperationsCancelTopic(
                operation_id=self.operation_id,
                operation_state_id=25, operation_state_name='Error',
                operation_type_id=26,
                client_id=self.client_id, account_id=self.account_id,
                amount=self.amount, currency=9991,
                reason_code_id=2006,  # Недостаточно бонусов
                error_message='Недостаточно бонусов для списания',
            ).check_message(self.cancel_message)
