from datetime import UTC, datetime, timedelta

import pytest
from hamcrest import assert_that, equal_to
from sqlalchemy.exc import OperationalError

import config
from bonus_service import service_name
from helpers import get_test_uuid
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_bonus_status_change import PortfolioBonusStatusChangeTopic


pytestmark = [
    allure.epic(service_name),
    allure.feature(config.bonus_status_change_topic),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'Job/producer: подходящий клиент с бонусом к активации, публикация clientId в Portfolio.Bonus.StatusChange.'
    ),
    allure.link('https://example.com/spec-redacted', name='Specification'),
    allure.severity('normal'),
    allure.tag(service_name),
    allure.tag('Kafka'),
    allure.tag('Bonus'),
]


class TestPositive:
    pytestmark = allure.story(
        'Positive. Producer публикует clientId в Portfolio.Bonus.StatusChange'
    )

    FROZEN_BONUS_STATUS_ID = 0
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
        self.client_id = get_test_uuid()
        self.account_id = get_test_uuid()
        self.job_name = 'BonusStatusChangeJobSchedule'

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

        with allure.step('Создаем запись Bonuses через ORM'):
            created_bonus = self.db._add_bonus(created_bonus)
            self.created_bonus_id = created_bonus.Id
            self.db.session.expunge_all()
            bonus_from_db = (
                self.db.session.query(self.db.model.Bonuses)
                .filter(self.db.model.Bonuses.Id == created_bonus.Id)
                .one()
            )
            assert_that(
                bonus_from_db.StatusId,
                equal_to(self.FROZEN_BONUS_STATUS_ID),
                'Подготовленная запись Bonuses должна быть в статусе FROZEN',
            )
            assert_that(
                bonus_from_db.ActivationDate < datetime.now(UTC),
                equal_to(True),
                'ActivationDate должен быть в прошлом, чтобы запись подходила для публикации в job',
            )

        allure_attach(bonus_from_db, 'prepared_bonus_for_status_change_job')

    def _send_message(self):
        with allure.step(f'Триггерим Quartz job {self.job_name}'):
            self.db.fire_job(job_name=self.job_name)

    def _after(self):
        with allure.step(
            f'Ожидаем сообщение в {config.bonus_status_change_topic} с clientId={self.client_id}'
        ):
            search_criteria = {'clientId': self.client_id}
            self.result_message = self.kafka.wait_message(
                topic_name=config.bonus_status_change_topic,
                search_criteria=search_criteria,
                timeout=120,
                time_shift=600,
            )
            allure_attach(self.result_message, 'bonus_status_change_message')

    def _cleanup(self):
        summary = {'deleted_bonus': 0, 'errors': []}
        with allure.step('Удаляем тестовую запись Bonuses'):
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
            except OperationalError as error:
                self.db.session.rollback()
                summary['errors'].append(repr(error))
                allure_attach(str(error), 'cleanup_delete_operational_error')
            except Exception as exc:
                self.db.session.rollback()
                summary['errors'].append(repr(exc))
        allure_attach(summary, 'cleanup_summary')

    @allure.title('Публикация clientId в Portfolio.Bonus.StatusChange (producer/job)')
    def test_job_publishes_client_id_to_bonus_status_change(self):
        with allure.step('Проверяем сообщение через topic checker'):
            topic = PortfolioBonusStatusChangeTopic(client_id=self.client_id)
            topic.check_message(self.result_message)
