from datetime import timezone
from uuid import uuid4

import pytest
from hamcrest import assert_that, equal_to, none, not_none

import config
from bonus_service import service_name
from helpers import datetime_now
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_balance_end_of_day import PortfolioBalanceEndOfDayTopic
from kafka_topics.internal.portfolio_bonus_operation import PortfolioBonusOperationTopic


pytestmark = [
    allure.epic(service_name),
    allure.feature(config.balance_end_of_day_topic),
    allure.label("microservice", service_name),
    allure.label("owner", "portfolio"),
    allure.description(
        "Проверка обработки сообщений BalanceEndOfDay и публикации события bonusAccrualForBalance."
    ),
    allure.link("https://example.com/reward-ledger-demo-spec", name="Specification"),
    allure.severity("normal"),
    allure.tag(service_name),
    allure.tag("Kafka"),
    allure.tag("Bonus"),
]


class TestPositive:
    pytestmark = allure.story(
        "Positive. Расчёт бонуса по сообщению BalanceEndOfDay"
    )

    RULE_ID_EXPECTED = 1

    @allure.title("Подготовка данных для тестов")
    @pytest.fixture(scope="class", autouse=True)
    def prepare(self, request, kafka, db):
        self = request.cls
        self.kafka = kafka
        self.db = db
        self.created_bonus_id = None
        try:
            self._before(self)
            self._send_message(self)
            self._after(self)
            yield
        finally:
            self._cleanup(self)

    def _before(self):
        with allure.step("Подготавливаем данные для BalanceEndOfDay"):
            now = datetime_now().astimezone(timezone.utc).replace(microsecond=0)

            self.client_id = str(uuid4())
            self.account_id = str(uuid4())
            self.account_type = 2
            self.balance = 5_000_000
            self.balance_day = now.isoformat(timespec="microseconds")

            self.bonus_rule = (
                self.db.session.query(self.db.model.AccrualRule)
                .filter(self.db.model.AccrualRule.RuleId == self.RULE_ID_EXPECTED)
                .one()
            )
            allure_attach(self.bonus_rule, "bonus_rule")

            self.expected_bonus = int((self.balance * self.bonus_rule.Percentage) // 365 // 100)

            self.request_id = str(uuid4())

            allure_attach(
                {
                    "clientId": self.client_id,
                    "accountId": self.account_id,
                    "accountType": self.account_type,
                    "balance": self.balance,
                    "balanceDay": self.balance_day,
                    "expectedBonus": self.expected_bonus,
                    "ruleIdExpected": self.RULE_ID_EXPECTED,
                },
                "balance_end_of_day_context",
            )

    def _send_message(self):
        with allure.step("Формируем сообщение BalanceEndOfDay"):
            context = {
                "client_id": self.client_id,
                "account_id": self.account_id,
                "account_type": self.account_type,
                "balance": self.balance,
                "balance_day": self.balance_day,
                "request_id": self.request_id,
            }
            topic = PortfolioBalanceEndOfDayTopic(context=context)
            message, key, headers = topic.set_message()
            allure_attach(message, "balance_end_of_day_message")

        with allure.step(
            f"Отправляем сообщение в топик {config.balance_end_of_day_topic}"
        ):
            self.kafka.send_message(
                topic=topic.name,
                message=message,
                key=key,
                headers=headers,
            )

    def _after(self):
        self._wait_for_bonus_message(self)
        self._load_bonus_row(self)

    def _wait_for_bonus_message(self):
        with allure.step(
            f"Ожидаем сообщение в топике {config.bonus_operation_topic}"
        ):
            search_criteria = {"bonusOperation": {"accountId": self.account_id}}

            self.bonus_message = self.kafka.wait_message(
                topic_name=config.bonus_operation_topic,
                search_criteria=search_criteria,
                timeout=120,
                time_shift=600,
            )

            allure_attach(self.bonus_message, "bonus_operation_message")

    def _load_bonus_row(self):
        with allure.step("Загружаем запись Bonuses по accountId"):
            self.bonus_row = (
                self.db.session.query(self.db.model.Bonuses)
                .filter(self.db.model.Bonuses.AccountId == self.account_id)
                .one()
            )
            self.created_bonus_id = self.bonus_row.Id
            allure_attach(
                self.bonus_row,
                "bonus_row",
            )
            assert_that(self.bonus_row.rule, not_none(), "Не загружено правило для бонуса")

    def _cleanup(self):
        summary = {"deleted_bonus": 0, "errors": []}
        try:
            self.db.session.rollback()
            bonus_id = getattr(self, "created_bonus_id", None)
            deleted = 0
            if bonus_id is not None:
                deleted = (
                    self.db.session.query(self.db.model.Bonuses)
                    .filter(self.db.model.Bonuses.Id == bonus_id)
                    .delete(synchronize_session=False)
                )
            elif getattr(self, "account_id", None) is not None:
                deleted = (
                    self.db.session.query(self.db.model.Bonuses)
                    .filter(self.db.model.Bonuses.AccountId == self.account_id)
                    .delete(synchronize_session=False)
                )
            summary["deleted_bonus"] = deleted
            self.db.session.commit()
        except Exception as exc:
            self.db.session.rollback()
            summary["errors"].append(repr(exc))
        allure_attach(summary, "cleanup_summary")

    @allure.title("Запись в Bonuses после BalanceEndOfDay")
    def test_bonus_row_created_after_balance_end_of_day(self):
        with allure.step("Запись Bonuses существует и согласована с расчётом"):
            assert_that(self.bonus_row, not_none())
            assert_that(self.bonus_row.RuleId, equal_to(self.bonus_rule.RuleId))
            assert_that(self.bonus_row.StatusId, equal_to(self.bonus_rule.BonusStatusId))
            assert_that(self.bonus_row.OperationId, none())
            assert_that(self.bonus_row.OperationAmount, equal_to(self.balance))
            assert_that(self.bonus_row.Amount, equal_to(self.expected_bonus))
            assert_that(self.bonus_row.CurrentBalance, equal_to(self.expected_bonus))
        with allure.step("Даты активации и истечения заполнены"):
            assert_that(self.bonus_row.ActivationDate, not_none())
            assert_that(self.bonus_row.ExpiredDate, not_none())

    @allure.title("Проверка сообщения Portfolio.Bonus.Operation")
    def test_bonus_operation_event_published(self):
        with allure.step("Сообщение должно быть получено"):
            assert_that(self.bonus_message, not_none(), "Сообщение отсутствует")
            allure_attach(self.bonus_message, "bonus_operation_message")

        with allure.step("Проверяем сообщение через topic checker"):
            topic = PortfolioBonusOperationTopic()
            topic.check_message(
                message=self.bonus_message,
                context={
                    "client_id": self.client_id,
                    "source_account_id": self.account_id,
                    "event_type_id": 1,
                    "balance_day": self.balance_day,
                    "expected_bonus": self.expected_bonus,
                    "bonus_row": self.bonus_row,
                    "bonus_rule": self.bonus_rule,
                },
            )
