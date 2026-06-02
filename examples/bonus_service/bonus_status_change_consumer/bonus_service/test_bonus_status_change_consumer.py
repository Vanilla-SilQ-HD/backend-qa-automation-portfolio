from datetime import UTC, datetime, timedelta
import time

import pytest
from hamcrest import assert_that, equal_to, is_not, less_than, not_none

import config
from bonus_service import service_name
from helpers import get_test_uuid, datetime_now
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_bonus_operation import PortfolioBonusOperationTopic
from kafka_topics.internal.portfolio_bonus_status_change import PortfolioBonusStatusChangeTopic


pytestmark = [
    allure.epic(service_name),
    allure.feature(config.bonus_status_change_topic),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'Обновление статуса бонуса в БД и публикация разморозки в Portfolio.Bonus.Operation.'
    ),
    allure.link('https://example.com/reward-ledger-demo-spec', name='Specification'),
    allure.severity('normal'),
    allure.tag(service_name),
    allure.tag('Kafka'),
    allure.tag('Bonus'),
]


class TestPositive:
    pytestmark = allure.story(
        'Positive. Изменение статуса бонуса'
    )

    FROZEN_BONUS_STATUS_ID = 0
    ACTIVE_BONUS_STATUS_ID = 1
    RULE_ID = 1

    @allure.title('Подготовка данных для тестов')
    @pytest.fixture(scope='class', autouse=True)
    def prepare(self, request, db, kafka):
        self = request.cls
        self.db = db
        self.kafka = kafka
        self.created_bonus_id = None
        try:
            self._before(self)
            self._send_message(self)
            self._after(self)
            yield
        finally:
            self._cleanup(self)

    def _before(self):
        with allure.step('Создаём бонус в статусе FROZEN'):
            self.client_id = get_test_uuid()
            self.account_id = get_test_uuid()
            utc_now = datetime.now(UTC).replace(microsecond=0)
            activation_date = utc_now - timedelta(hours=1)
            operation_date = utc_now
            operation_id = int(operation_date.timestamp())

            created_bonus = self.db.model.Bonuses()
            created_bonus.set_values(
                client_id=self.client_id,
                account_id=self.account_id,
                rule_id=self.RULE_ID,
                status_id=self.FROZEN_BONUS_STATUS_ID,
                operation_id=operation_id,
                operation_date=operation_date,
                amount=100,
                current_balance=100,
                operation_amount=1000,
                activation_date=activation_date,
                expired_date=utc_now + timedelta(days=2),
                created_on=operation_date,
                created_by='autotest',
                modified_on=operation_date,
                modified_by='autotest',
            )

            created_bonus = self.db._add_bonus(created_bonus)
            self.created_bonus_id = created_bonus.Id
            self.db.session.expunge_all()
            self.bonus_before = (
                self.db.session.query(self.db.model.Bonuses)
                .filter(self.db.model.Bonuses.Id == self.created_bonus_id)
                .one()
            )
            allure_attach(self.bonus_before, 'bonus_before')
            self.db.session.expunge(self.bonus_before)

    def _send_message(self):
        status_change_topic = PortfolioBonusStatusChangeTopic(
            client_id=self.client_id,
        )
        message, key, headers = status_change_topic.set_message()

        with allure.step('Отправляем сообщение в Portfolio.Bonus.StatusChange'):
            allure_attach(message, 'bonus_status_change_message')
            self.kafka.send_message(
                topic=status_change_topic.name,
                message=message,
                key=key,
                headers=headers,
            )

    def _after(self):
        with allure.step('Загружаем данные после обработки события'):
            self.bonus_after = (
                self.db.session.query(self.db.model.Bonuses)
                .filter_by(
                    ClientId=self.bonus_before.ClientId,
                    AccountId=self.bonus_before.AccountId,
                )
                .one()
            )
            for _ in range(30):
                if self.bonus_after.StatusId != self.bonus_before.StatusId:
                    break
                time.sleep(0.5)
                self.db.session.refresh(self.bonus_after)
            allure_attach(self.bonus_after, 'bonus_after')

    def _cleanup(self):
        summary = {'deleted_bonus': 0, 'errors': []}
        try:
            self.db.session.rollback()
            if getattr(self, 'created_bonus_id', None) is not None:
                deleted = (
                    self.db.session.query(self.db.model.Bonuses)
                    .filter(self.db.model.Bonuses.Id == self.created_bonus_id)
                    .delete(synchronize_session=False)
                )
                summary['deleted_bonus'] = deleted
                self.db.session.commit()
        except Exception as exc:
            self.db.session.rollback()
            summary['errors'].append(repr(exc))
        allure_attach(summary, 'cleanup_summary')

    @allure.title('Обновление Bonuses в БД')
    def test_bonus_status_updated(self):
        with allure.step('Проверяем изменения в Bonuses'):
            allure_attach(self.bonus_before, 'bonus_before')
            assert_that(
                self.bonus_before.StatusId,
                equal_to(self.FROZEN_BONUS_STATUS_ID),
                'Перед обработкой события бонус должен быть в статусе FROZEN',
            )
            assert_that(
                self.bonus_after.StatusId,
                equal_to(self.ACTIVE_BONUS_STATUS_ID),
                'После обработки события бонус должен перейти в статус ACTIVE',
            )
            assert_that(
                self.bonus_after.ModifiedBy,
                equal_to('portfolio.bonus.service'),
                'После обработки события ModifiedBy должен быть portfolio.bonus.service',
            )
            assert_that(
                self.bonus_after.ModifiedOn,
                is_not(equal_to(self.bonus_before.ModifiedOn)),
                'После обработки события ModifiedOn должен измениться',
            )
            assert_that(
                abs(datetime_now() - self.bonus_after.ModifiedOn),
                less_than(timedelta(minutes=5)),
                'После обработки события ModifiedOn должен быть близок к текущему времени',
            )
            assert_that(
                self.bonus_after.is_equal(
                    self.bonus_before,
                    ignored=('StatusId', 'ModifiedBy', 'ModifiedOn'),
                ),
                'После обработки события в записи Bonuses должны измениться только StatusId и служебные поля',
            )

    @allure.title(f'Сообщение в {config.bonus_operation_topic}')
    def test_bonus_operation_event_published(self):
        bonus_operation_topic = PortfolioBonusOperationTopic()
        with allure.step(f'Проверяем запись в топике {config.bonus_operation_topic}'):
            result_message = self.kafka.wait_message(
                topic_name=config.bonus_operation_topic,
                search_criteria={
                    'trace': {'notifyId': 'BonusUnfrozen'},
                    'bonusOperation': {'clientId': self.client_id},
                },
            )
            assert_that(
                self.bonus_after.rule,
                not_none(),
                f'Не найдено правило AccrualRule для RuleId={self.bonus_after.RuleId}',
            )
            allure_attach(result_message, 'portfolio_bonus_operation_message')

        with allure.step('Проверяем сообщение через topic checker'):
            bonus_operation_topic.check_message(
                message=result_message,
                bonus=self.bonus_after,
            )
