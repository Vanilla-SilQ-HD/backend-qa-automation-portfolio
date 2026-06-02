import random
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from hamcrest import assert_that, equal_to, greater_than_or_equal_to, not_none
from sqlalchemy.exc import DBAPIError, OperationalError

import config
from bonus_service import service_name
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_bonus_operation import PortfolioBonusOperationTopic
from kafka_topics.internal.portfolio_operations_events import PortfolioOperationsEventsTopic


BONUS_CURRENCY_ID = 9991
AMOUNT_MIN = 1000
AMOUNT_MAX = 9000

BONUS_STATUS_ACTIVE = 1

REWARD_CREDIT_BONUS_TYPE_ID = 26
REWARD_CREDIT_BONUS_TYPE_NAME = "RewardCreditBonus"

OPERATION_STATE_CHECK_BALANCE_ID = 11
OPERATION_STATE_CHECK_BALANCE_NAME = "CheckBalance"
OPERATION_STATE_ERROR_ID = 25
OPERATION_STATE_ERROR_NAME = "Error"

WITHDRAWAL_STATUS_COMPLETED = 1
WITHDRAWAL_STATUS_CANCELED = 2

REASON_CODE_ROLLBACK = 500
REASON_MESSAGE_ROLLBACK = "Rollback erroneous operation"

BONUS_DESCRIPTION = "Пополнение с бонусного счета"


def _headers_to_attach(headers):
    return {k: v.decode() if isinstance(v, bytes) else v for k, v in headers}


pytestmark = [
    allure.epic(service_name),
    allure.feature(config.portfolio_operations_events_topic),
    allure.label("microservice", service_name),
    allure.label("owner", "portfolio"),
    allure.description(
        "Проверка обработки BonusService negative-события Portfolio.Operations.Events: "
        "отмена списания бонусов, восстановление бонусного остатка и публикация Portfolio.Bonus.Operation."
    ),
    allure.link("https://example.com/reward-ledger-demo-spec", name="Specification"),
    allure.severity("critical"),
    allure.tag(service_name),
    allure.tag("Kafka"),
    allure.tag("Portfolio.Operations.Events"),
    allure.tag("BonusExchange"),
]


class TestNegative:
    pytestmark = allure.story("Negative. Обработка Portfolio.Operations.Events state=25")

    @allure.title("Подготовка данных для тестов")
    @pytest.fixture(scope="class", autouse=True)
    def prepare(self, request, db, kafka):
        self = request.cls
        self.db = db
        self.kafka = kafka
        self.created_bonus_id = None
        self.withdrawall_operation_db_id = None
        self.operation_id = None
        try:
            self._before(self)
            self._send_message(self)
            self._after(self)
            self._send_negative_message(self)
            self._after_negative_message(self)
            yield
        finally:
            self._cleanup(self)

    def _before(self):
        with allure.step("Генерация идентификаторов и подбор AccrualRule"):
            self.client_id = str(uuid4())
            self.account_id = str(uuid4())
            self.operation_id = random.randint(1_000_000_000, 2_100_000_000)
            self.amount = random.randint(AMOUNT_MIN, AMOUNT_MAX)
            self.external_user_ref = random.choice(config.demo_user_refs)

            now = datetime.now(UTC).replace(microsecond=0)
            m = self.db.model

            rule = (
                self.db.session.query(m.AccrualRule)
                .filter(
                    m.AccrualRule.CurrencyId == BONUS_CURRENCY_ID,
                    m.AccrualRule.ValidFrom < now,
                    m.AccrualRule.ValidTo > now,
                )
                .order_by(m.AccrualRule.RuleId)
                .first()
            )
            assert_that(
                rule,
                not_none(),
                f"Не найден валидный AccrualRule для CurrencyId={BONUS_CURRENCY_ID}",
            )
            self.accrual_rule = rule
            allure_attach(
                {
                    "client_id": self.client_id,
                    "account_id": self.account_id,
                    "operation_id": self.operation_id,
                    "amount": self.amount,
                    "externalUserRef": self.external_user_ref,
                    "rule_id": self.accrual_rule.RuleId,
                    "rule_currency_id": self.accrual_rule.CurrencyId,
                },
                "operations_events_negative_context",
            )

        with allure.step("Создание записи Bonuses (ACTIVE, достаточный остаток)"):
            activation = now - timedelta(hours=1)
            expired = now + timedelta(days=30)
            bonus = m.Bonuses(
                ClientId=self.client_id,
                AccountId=self.account_id,
                RuleId=self.accrual_rule.RuleId,
                StatusId=BONUS_STATUS_ACTIVE,
                OperationId=None,
                OperationDate=now,
                Amount=self.amount * 2,
                CurrentBalance=self.amount * 2,
                OperationAmount=self.amount,
                ActivationDate=activation,
                ExpiredDate=expired,
                CreatedOn=now,
                CreatedBy="Portfolio.Autotests",
                ModifiedOn=now,
                ModifiedBy="Portfolio.Autotests",
            )
            self.db.session.add(bonus)
            self.db.session.commit()
            self.db.session.refresh(bonus)
            self.created_bonus_id = bonus.Id
            self.bonus_before_positive = (
                self.db.session.query(m.Bonuses)
                .filter(m.Bonuses.Id == self.created_bonus_id)
                .one()
            )
            self.db.session.expunge(self.bonus_before_positive)
            allure_attach(self.bonus_before_positive, "bonus_before_positive")

    def _send_message(self):
        with allure.step("Отправка Portfolio.Operations.Events state=11 (CheckBalance)"):
            topic = PortfolioOperationsEventsTopic(
                operation_id=self.operation_id,
                operation_state_id=OPERATION_STATE_CHECK_BALANCE_ID,
                operation_state_name=OPERATION_STATE_CHECK_BALANCE_NAME,
                operation_type_id=REWARD_CREDIT_BONUS_TYPE_ID,
                operation_type_name=REWARD_CREDIT_BONUS_TYPE_NAME,
                client_id=self.client_id,
                account_id=self.account_id,
                amount=self.amount,
                currency=BONUS_CURRENCY_ID,
                fee=0,
                description=BONUS_DESCRIPTION,
                external_user_ref=self.external_user_ref,
            )
            message, key, headers = topic.set_message()
            allure_attach(
                {"message": message, "key": key, "headers": _headers_to_attach(headers)},
                "positive_portfolio_operations_event",
            )
            self.kafka.send_message(
                topic=topic.name,
                message=message,
                key=key,
                headers=headers,
            )

    def _after(self):
        m = self.db.model
        deadline = time.time() + 120

        with allure.step("Ожидание WithdrawallOperation по operationId"):
            wop = None
            while time.time() < deadline:
                self.db.session.expire_all()
                wop = (
                    self.db.session.query(m.WithdrawallOperation)
                    .filter(m.WithdrawallOperation.OperationId == self.operation_id)
                    .one_or_none()
                )
                if wop is not None:
                    break
                time.sleep(0.5)
            assert_that(wop, not_none(), "WithdrawallOperation не создана после positive-события")
            self.withdrawall_operation_db_id = wop.Id
            assert_that(
                wop.WithdrawalStatusId,
                equal_to(WITHDRAWAL_STATUS_COMPLETED),
                "После CheckBalance ожидается WithdrawalStatusId=Completed(1)",
            )
            self.withdrawal_operation_before_negative = wop
            self.db.session.expunge(self.withdrawal_operation_before_negative)

        with allure.step("Ожидание WithdrawallOperationDetail"):
            deadline_d = time.time() + 60
            details = []
            while time.time() < deadline_d:
                self.db.session.expire_all()
                details = (
                    self.db.session.query(m.WithdrawallOperationDetail)
                    .filter(
                        m.WithdrawallOperationDetail.WithdrawallOperationId == self.withdrawall_operation_db_id
                    )
                    .all()
                )
                if details:
                    break
                time.sleep(0.5)
            assert_that(len(details), greater_than_or_equal_to(1), "Ожидается минимум одна деталь списания")
            self.withdrawal_operation_details_before_negative = details
            allure_attach(
                [
                    {
                        "Id": d.Id,
                        "WithdrawallOperationId": d.WithdrawallOperationId,
                        "WithdrawallId": getattr(d, "WithdrawallId", None),
                        "BonusId": d.BonusId,
                        "AmountUsed": d.AmountUsed,
                    }
                    for d in details
                ],
                "withdrawal_operation_details_before_negative",
            )

        with allure.step("Перечитать Bonuses после positive-flow"):
            self.bonus_after_positive = (
                self.db.session.query(m.Bonuses)
                .filter(m.Bonuses.Id == self.created_bonus_id)
                .one()
            )
            self.db.session.expunge(self.bonus_after_positive)
            allure_attach(self.bonus_after_positive, "bonus_after_positive")

        with allure.step("Ожидание Portfolio.Bonus.Operation bonusExchange"):
            self.portfolio_bonus_operation_positive = self.kafka.wait_message(
                topic_name=config.bonus_operation_topic,
                search_criteria={
                    "bonusOperation": {
                        "operationId": self.operation_id,
                    }
                },
                timeout=30,
                time_shift=600,
            )
            allure_attach(self.portfolio_bonus_operation_positive, "portfolio_bonus_operation_positive")

        allure_attach(self.withdrawal_operation_before_negative, "withdrawal_operation_before_negative")

    def _send_negative_message(self):
        with allure.step("Отправка Portfolio.Operations.Events state=25 (Error)"):
            topic = PortfolioOperationsEventsTopic(
                operation_id=self.operation_id,
                operation_state_id=OPERATION_STATE_ERROR_ID,
                operation_state_name=OPERATION_STATE_ERROR_NAME,
                operation_type_id=REWARD_CREDIT_BONUS_TYPE_ID,
                operation_type_name=REWARD_CREDIT_BONUS_TYPE_NAME,
                client_id=self.client_id,
                account_id=self.account_id,
                amount=self.amount,
                currency=BONUS_CURRENCY_ID,
                fee=0,
                description=BONUS_DESCRIPTION,
                external_user_ref=self.external_user_ref,
                reason_code_id=REASON_CODE_ROLLBACK,
                error_message=REASON_MESSAGE_ROLLBACK,
            )
            message, key, headers = topic.set_message()
            allure_attach(
                {"message": message, "key": key, "headers": _headers_to_attach(headers)},
                "negative_portfolio_operations_event",
            )
            self.kafka.send_message(
                topic=topic.name,
                message=message,
                key=key,
                headers=headers,
            )

    def _after_negative_message(self):
        m = self.db.model
        deadline = time.time() + 120

        with allure.step("Ожидание WithdrawallOperation в статусе Canceled (2)"):
            last_db_error = None
            wop = None
            while time.time() < deadline:
                try:
                    self.db.session.expire_all()
                    wop = (
                        self.db.session.query(m.WithdrawallOperation)
                        .filter(m.WithdrawallOperation.OperationId == self.operation_id)
                        .one_or_none()
                    )
                except (OperationalError, DBAPIError) as exc:
                    self.db.session.rollback()
                    last_db_error = repr(exc)
                    time.sleep(1)
                    continue
                if wop is not None and wop.WithdrawalStatusId == WITHDRAWAL_STATUS_CANCELED:
                    break
                time.sleep(0.5)
            if wop is None or wop.WithdrawalStatusId != WITHDRAWAL_STATUS_CANCELED:
                allure_attach({"last_db_error": last_db_error}, "negative_wait_db_error")
            assert_that(wop, not_none(), "WithdrawallOperation не найдена после negative-события")
            self.withdrawal_operation_after_negative = wop
            self.db.session.expunge(self.withdrawal_operation_after_negative)

        with allure.step("Перечитать WithdrawallOperationDetails после negative"):
            details = (
                self.db.session.query(m.WithdrawallOperationDetail)
                .filter(
                    m.WithdrawallOperationDetail.WithdrawallOperationId == self.withdrawall_operation_db_id
                )
                .all()
            )
            self.withdrawal_operation_details_after_negative = details
            allure_attach(
                [
                    {
                        "Id": d.Id,
                        "WithdrawallOperationId": d.WithdrawallOperationId,
                        "WithdrawallId": getattr(d, "WithdrawallId", None),
                        "BonusId": d.BonusId,
                        "AmountUsed": d.AmountUsed,
                    }
                    for d in details
                ],
                "withdrawal_operation_details_after_negative",
            )

        with allure.step("Перечитать Bonuses после negative-flow"):
            self.bonus_after_negative = (
                self.db.session.query(m.Bonuses)
                .filter(m.Bonuses.Id == self.created_bonus_id)
                .one()
            )
            self.db.session.expunge(self.bonus_after_negative)
            allure_attach(self.bonus_after_negative, "bonus_after_negative")

        with allure.step("Ожидание Portfolio.Bonus.Operation bonusExchangeCancel"):
            deadline_kafka = time.time() + 120
            cancel_message = None
            messages = []
            while time.time() < deadline_kafka:
                messages = self.kafka.get_messages(
                    topic_name=config.bonus_operation_topic,
                    search_criteria={
                        "bonusOperation": {
                            "operationId": self.operation_id,
                        }
                    },
                    time_shift=600,
                    max_records=600,
                )
                for message in messages:
                    headers = {k: v.decode() if isinstance(v, bytes) else v for k, v in (message.headers or [])}
                    value = message.value or {}
                    trace = value.get("trace") or {}
                    header_event_type = headers.get("eventType")
                    notify_id = trace.get("notifyId") if isinstance(trace, dict) else None
                    if header_event_type == "bonusExchangeCancel" or notify_id == "bonusExchangeCancel":
                        cancel_message = message
                        break
                if cancel_message is not None:
                    break
                time.sleep(2)

            self.portfolio_bonus_operation_cancel = cancel_message
            allure_attach(self.portfolio_bonus_operation_cancel, "portfolio_bonus_operation_cancel")
            if cancel_message is None:
                allure_attach(messages, "portfolio_bonus_operation_messages_by_operation_id")

    def _cleanup(self):
        try:
            self.db.session.rollback()
        except Exception:
            pass
        summary = {"deleted_details": 0, "deleted_withdrawal": 0, "deleted_bonus": 0, "errors": []}
        m = self.db.model
        try:
            with allure.step("Cleanup: WithdrawallOperationDetails / WithdrawallOperation / Bonuses"):
                if self.withdrawall_operation_db_id is not None:
                    n = (
                        self.db.session.query(m.WithdrawallOperationDetail)
                        .filter(
                            m.WithdrawallOperationDetail.WithdrawallOperationId
                            == self.withdrawall_operation_db_id
                        )
                        .delete(synchronize_session=False)
                    )
                    summary["deleted_details"] = n
                    self.db.session.commit()

                if self.operation_id is not None:
                    n2 = (
                        self.db.session.query(m.WithdrawallOperation)
                        .filter(m.WithdrawallOperation.OperationId == self.operation_id)
                        .delete(synchronize_session=False)
                    )
                    summary["deleted_withdrawal"] = n2
                    self.db.session.commit()

                if self.created_bonus_id is not None:
                    n3 = (
                        self.db.session.query(m.Bonuses)
                        .filter(m.Bonuses.Id == self.created_bonus_id)
                        .delete(synchronize_session=False)
                    )
                    summary["deleted_bonus"] = n3
                    self.db.session.commit()
        except Exception as exc:
            try:
                self.db.session.rollback()
            except Exception:
                pass
            summary["errors"].append(repr(exc))
        allure_attach(summary, "cleanup_summary")

    @allure.title("WithdrawallOperation переведена в Canceled после negative-события")
    def test_withdrawal_operation_cancelled_after_negative_event(self):
        assert_that(self.withdrawal_operation_before_negative, not_none(), "Нет снимка withdrawal до negative")
        assert_that(
            self.withdrawal_operation_before_negative.WithdrawalStatusId,
            equal_to(WITHDRAWAL_STATUS_COMPLETED),
            "До negative ожидался Completed",
        )
        assert_that(self.withdrawal_operation_after_negative, not_none())
        assert_that(
            self.withdrawal_operation_after_negative.WithdrawalStatusId,
            equal_to(WITHDRAWAL_STATUS_CANCELED),
            "После negative ожидается Canceled",
        )

    @allure.title("Статус и остаток бонусного пакета корректны после negative-события")
    def test_bonus_balance_restored_after_negative_event(self):
        allure_attach(
            {
                "before_positive": getattr(self.bonus_before_positive, "CurrentBalance", None),
                "after_positive": getattr(self.bonus_after_positive, "CurrentBalance", None),
                "after_negative": getattr(self.bonus_after_negative, "CurrentBalance", None),
                "status_after_negative": getattr(self.bonus_after_negative, "StatusId", None),
            },
            "bonus_balance_timeline",
        )
        assert_that(self.bonus_after_negative, not_none())
        assert_that(
            self.bonus_after_negative.StatusId,
            equal_to(BONUS_STATUS_ACTIVE),
            "После сторно бонус должен быть ACTIVE",
        )
        assert_that(
            self.bonus_after_negative.CurrentBalance,
            equal_to(self.bonus_before_positive.CurrentBalance),
            "После сторно CurrentBalance должен вернуться к исходному значению",
        )

    @allure.title("Опубликовано Portfolio.Bonus.Operation с bonusExchangeCancel")
    def test_bonus_exchange_cancel_message_published(self):
        assert_that(self.portfolio_bonus_operation_cancel, not_none(), "Сообщение cancel не получено из Kafka")
        topic = PortfolioBonusOperationTopic()
        topic.check_message(
            self.portfolio_bonus_operation_cancel,
            context={
                "expected_header_event_type": "bonusExchangeCancel",
                "client_id": self.client_id,
                "account_id": self.account_id,
                "operation_id": self.operation_id,
                "amount": self.amount,
                "bonus_id": self.created_bonus_id,
                "expected_currency": BONUS_CURRENCY_ID,
            },
        )
