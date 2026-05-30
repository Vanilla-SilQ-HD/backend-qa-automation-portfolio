from datetime import datetime, timedelta, timezone

import pytest
from hamcrest import assert_that, equal_to, greater_than, instance_of

from bonus_service import requests_session, service_name
from bonus_service.response_models.bonus_accruals_get import (
    BonusAccrualsErrorResponse,
    BonusAccrualsResponse,
)
from helpers.allure_client import allure, allure_attach
from helpers import check_assert_that, datetime_now


pytestmark = [
    allure.epic(service_name),
    allure.feature("GET /api/v1/bonus/bonusAccruals"),
    allure.label("microservice", service_name),
    allure.label("owner", "portfolio"),
    allure.description("Метод предназначен для получения текущих правил начисления бонусов"),
    allure.link("https://example.com/spec-redacted", name="Specification"),
    allure.severity("normal"),
    allure.tag(service_name),
    allure.tag("BonusAccruals"),
    allure.tag("API"),
]


class TestPositive:
    pytestmark = allure.story("Positive. Успешный ответ")

    @allure.title("Подготовка данных для тестов")
    @pytest.fixture(scope="class", autouse=True)
    def prepare(self, request, db):
        self = request.cls
        self.db = db
        self._before(self)
        yield
        self._cleanup(self)

    def _before(self):
        self.base_url = f"{requests_session.url}v1/bonus/bonusAccruals"

        self.active_rule_ids = (10_000_000, 10_000_001)
        self.inactive_rule_id = 10_000_002
        self.prepared_rule_ids = self.active_rule_ids + (self.inactive_rule_id,)
        self.now = datetime_now().replace(microsecond=0)

        active_valid_from = self.now - timedelta(days=1)
        active_valid_to = self.now + timedelta(days=1)
        inactive_valid_from = self.now - timedelta(days=30)
        inactive_valid_to = self.now - timedelta(days=1)

        rule_type_ids = [
            row[0]
            for row in self.db.session.query(self.db.model.RuleType.Id)
            .order_by(self.db.model.RuleType.Id)
            .limit(2)
            .all()
        ]
        assert_that(
            len(rule_type_ids),
            greater_than(1),
            "Для сценария требуется минимум 2 RuleType в БД "
            "(active и inactive должны иметь разные RuleType)",
        )
        self.rule_type_filter_id = rule_type_ids[0]
        self.rule_type_inactive_id = rule_type_ids[1]

        with allure.step("Подготовка AccrualRules для детерминированных проверок"):
            self.db._delete_accrual_rules_by_ids(self.prepared_rule_ids)

            rule_1 = self.db.model.AccrualRule()
            rule_1.RuleId = self.active_rule_ids[0]
            rule_1.RuleTypeId = self.rule_type_filter_id
            rule_1.AccountTypeId = 2
            rule_1.Percentage = 10.0
            rule_1.OperationCategoryId = None
            rule_1.MinAmountForBonus = None
            rule_1.MaxAmountForBonus = None
            rule_1.CurrencyId = 9991
            rule_1.BonusStatusId = 0
            rule_1.BurningPeriodMonths = None
            rule_1.BurningPeriodDay = None
            rule_1.ActivationMonth = None
            rule_1.ActivationDay = None
            rule_1.ValidFrom = active_valid_from
            rule_1.ValidTo = active_valid_to
            rule_1.CreatedOn = self.now
            rule_1.CreatedBy = "autotest"
            rule_1.ModifiedOn = self.now
            rule_1.ModifiedBy = "autotest"

            rule_2 = self.db.model.AccrualRule()
            rule_2.RuleId = self.active_rule_ids[1]
            rule_2.RuleTypeId = self.rule_type_filter_id
            rule_2.AccountTypeId = 2
            rule_2.Percentage = 20.0
            rule_2.OperationCategoryId = None
            rule_2.MinAmountForBonus = None
            rule_2.MaxAmountForBonus = None
            rule_2.CurrencyId = 9991
            rule_2.BonusStatusId = 0
            rule_2.BurningPeriodMonths = None
            rule_2.BurningPeriodDay = None
            rule_2.ActivationMonth = None
            rule_2.ActivationDay = None
            rule_2.ValidFrom = active_valid_from
            rule_2.ValidTo = active_valid_to
            rule_2.CreatedOn = self.now
            rule_2.CreatedBy = "autotest"
            rule_2.ModifiedOn = self.now
            rule_2.ModifiedBy = "autotest"

            rule_3 = self.db.model.AccrualRule()
            rule_3.RuleId = self.inactive_rule_id
            rule_3.RuleTypeId = self.rule_type_inactive_id
            rule_3.AccountTypeId = 2
            rule_3.Percentage = 30.0
            rule_3.OperationCategoryId = None
            rule_3.MinAmountForBonus = None
            rule_3.MaxAmountForBonus = None
            rule_3.CurrencyId = 9991
            rule_3.BonusStatusId = 2
            rule_3.BurningPeriodMonths = None
            rule_3.BurningPeriodDay = None
            rule_3.ActivationMonth = None
            rule_3.ActivationDay = None
            rule_3.ValidFrom = inactive_valid_from
            rule_3.ValidTo = inactive_valid_to
            rule_3.CreatedOn = self.now
            rule_3.CreatedBy = "autotest"
            rule_3.ModifiedOn = self.now
            rule_3.ModifiedBy = "autotest"

            self.db._add_accrual_rules([rule_1, rule_2, rule_3])
            self.db.session.expunge_all()
            self.prepared_rules = (
                self.db.session.query(self.db.model.AccrualRule)
                .filter(self.db.model.AccrualRule.RuleId.in_(self.prepared_rule_ids))
                .all()
            )
            allure_attach(self.prepared_rules, "prepared_accrual_rules")

    def _cleanup(self):
        with allure.step("Cleanup подготовленных AccrualRules после выполнения тестов"):
            self.db._delete_accrual_rules_by_ids(self.prepared_rule_ids)

    def _send_request(self, params=None):
        with allure.step(f"Отправка GET запроса с параметрами: {params}"):
            self.response = requests_session.get(
                self.base_url,
                params=params,
            )

    def _after(self):
        with allure.step("Проверка HTTP статуса = 200"):
            assert_that(self.response.status_code, equal_to(200))

        with allure.step("Проверка тела ответа и схемы через pydantic модель"):
            self.response_body = self.response.json()
            self.response_model = BonusAccrualsResponse(**self.response_body)
            self.rules = self.response_model.rules
            assert_that(self.rules, instance_of(list))
            self.raw_rules_by_id = {
                rule["ruleId"]: rule for rule in self.response_body["rules"]
            }

    @staticmethod
    def _normalize_response_datetime(value):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed.replace(microsecond=0)

    @staticmethod
    def _normalize_db_datetime(value):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.replace(microsecond=0)

    def _assert_response_rule_matches_db(self, response_rule, db_rule, raw_rule):
        check_assert_that(
            response_rule.ruleType,
            equal_to(db_rule.RuleType.Name),
            "Некорректное значение ruleType",
        )
        check_assert_that(
            response_rule.accountType,
            equal_to("Накопительный счет"),
            "Некорректное значение accountType",
        )
        check_assert_that(
            response_rule.bonusPercentage,
            equal_to(float(db_rule.Percentage)),
            "Некорректное значение bonusPercentage",
        )
        if "currency" in raw_rule:
            check_assert_that(
                response_rule.currency,
                equal_to(db_rule.CurrencyId),
                "Некорректное значение currency",
            )
        if "bonusStatus" in raw_rule:
            check_assert_that(
                response_rule.bonusStatus,
                equal_to(db_rule.BonusStatusId),
                "Некорректное значение bonusStatus",
            )
        check_assert_that(
            self._normalize_response_datetime(response_rule.validFrom),
            equal_to(self._normalize_db_datetime(db_rule.ValidFrom)),
            "Некорректное значение validFrom",
        )
        check_assert_that(
            self._normalize_response_datetime(response_rule.validTo),
            equal_to(self._normalize_db_datetime(db_rule.ValidTo)),
            "Некорректное значение validTo",
        )
        if "isActive" in raw_rule:
            expected_is_active = db_rule.ValidFrom <= self.now <= db_rule.ValidTo
            check_assert_that(
                response_rule.isActive,
                equal_to(expected_is_active),
                "Некорректное значение isActive",
            )

    @allure.title("Успешный ответ без фильтров")
    def test_without_filters(self):
        self._send_request()
        self._after(self)

        rules = self.rules
        rules_by_id = {rule.ruleId: rule for rule in rules}
        returned_rule_ids = set(rules_by_id)

        with allure.step("Состав prepared-правил в ответе без фильтров"):
            returned_prepared_ids = returned_rule_ids.intersection(set(self.prepared_rule_ids))
            assert_that(returned_prepared_ids, equal_to(set(self.active_rule_ids)))

        with allure.step("Глубокая сверка подготовленных активных правил с БД"):

            db_active_rules = (
                self.db.session.query(self.db.model.AccrualRule)
                .filter(self.db.model.AccrualRule.RuleId.in_(self.active_rule_ids))
                .filter(self.db.model.AccrualRule.ValidFrom <= self.now)
                .filter(self.db.model.AccrualRule.ValidTo >= self.now)
                .all()
            )
            allure_attach(db_active_rules, "db_active_rules")
            assert_that(
                {rule.RuleId for rule in db_active_rules},
                equal_to(set(self.active_rule_ids)),
            )

            db_rules_by_id = {rule.RuleId: rule for rule in db_active_rules}

            for active_rule_id in self.active_rule_ids:
                assert_that(
                    active_rule_id in rules_by_id,
                    equal_to(True),
                    f"В ответе отсутствует подготовленное активное правило {active_rule_id}",
                )
                db_rule = db_rules_by_id[active_rule_id]
                response_rule = rules_by_id[active_rule_id]
                self._assert_response_rule_matches_db(
                    response_rule,
                    db_rule,
                    self.raw_rules_by_id[active_rule_id],
                )

    @allure.title("Фильтрация по ruleType и accountType")
    def test_filter_by_rule_type_and_account_type(self):
        account_type = 2
        self._send_request(
            params={
                "ruleType": self.rule_type_filter_id,
                "accountType": account_type,
            }
        )
        self._after(self)

        with allure.step("Сверка полей ответа с БД для отфильтрованных правил"):
            db_rules = (
                self.db.session.query(self.db.model.AccrualRule)
                .filter_by(
                    RuleTypeId=self.rule_type_filter_id,
                    AccountTypeId=account_type,
                )
                .filter(self.db.model.AccrualRule.ValidFrom <= self.now)
                .filter(self.db.model.AccrualRule.ValidTo >= self.now)
                .all()
            )
            allure_attach(db_rules, "db_filtered_rules")
            expected_rule_ids = {rule.RuleId for rule in db_rules}
            returned_rule_ids = {rule.ruleId for rule in self.rules}
            assert_that(returned_rule_ids, equal_to(expected_rule_ids))

            db_rules_by_id = {rule.RuleId: rule for rule in db_rules}
            for rule in self.rules:
                db_rule = db_rules_by_id[rule.ruleId]
                self._assert_response_rule_matches_db(
                    rule,
                    db_rule,
                    self.raw_rules_by_id[rule.ruleId],
                )

    @allure.title("Проверка onlyActive=False")
    def test_only_active_false(self):
        with allure.step("Базовый запрос без onlyActive: среди prepared только 2 активных"):
            self._send_request()
            self._after(self)
            default_rules = self.response_model.rules
            default_prepared_ids = {
                rule.ruleId for rule in default_rules
            }.intersection(set(self.prepared_rule_ids))
            assert_that(default_prepared_ids, equal_to(set(self.active_rule_ids)))

        self._send_request(params={"onlyActive": "false"})
        self._after(self)

        with allure.step("Проверяем состав подготовленных правил в ответе"):
            returned_rule_ids = {rule.ruleId for rule in self.rules}
            returned_prepared_ids = returned_rule_ids.intersection(set(self.prepared_rule_ids))
            assert_that(
                returned_prepared_ids,
                equal_to(set(self.active_rule_ids)),
                "В ответе должны быть только активные подготовленные правила",
            )
            assert_that(
                self.inactive_rule_id not in returned_prepared_ids,
                equal_to(True),
                "Истёкшее подготовленное правило не должно попасть в ответ",
            )

        with allure.step("Сверяем подготовленные активные правила с БД"):
            rules_by_id = {rule.ruleId: rule for rule in self.rules}
            db_prepared_rules = (
                self.db.session.query(self.db.model.AccrualRule)
                .filter(self.db.model.AccrualRule.RuleId.in_(self.active_rule_ids))
                .all()
            )
            allure_attach(db_prepared_rules, "db_prepared_rules")
            assert_that(
                {rule.RuleId for rule in db_prepared_rules},
                equal_to(set(self.active_rule_ids)),
            )

            db_rules_by_id = {rule.RuleId: rule for rule in db_prepared_rules}
            for rule_id in self.active_rule_ids:
                self._assert_response_rule_matches_db(
                    rules_by_id[rule_id],
                    db_rules_by_id[rule_id],
                    self.raw_rules_by_id[rule_id],
                )


class TestNegativeBadRequest:
    pytestmark = allure.story("Negative. Некорректные параметры запроса")

    @allure.title("Подготовка данных для тестов")
    @pytest.fixture(scope="class", autouse=True)
    def prepare(self, request):
        self = request.cls
        self._before(self)

    def _before(self):
        self.base_url = f"{requests_session.url}v1/bonus/bonusAccruals"

    def _send_request(self, params=None):
        with allure.step(f"Отправка GET запроса с параметрами: {params}"):
            self.response = requests_session.get(
                self.base_url,
                params=params,
            )

    def _after(self):
        with allure.step("Проверка HTTP статуса = 400"):
            assert_that(self.response.status_code, equal_to(400))

        with allure.step("Проверка структуры ошибки"):
            body = self.response.json()
            error_response = BonusAccrualsErrorResponse(**body)
            assert_that(error_response.message, equal_to("Ошибка валидации"))
            assert_that(error_response.warnings, instance_of(list))
            assert_that(bool(error_response.warnings), equal_to(True))

    @allure.title("Некорректное значение ruleType")
    def test_invalid_rule_type(self):
        params = {"ruleType": "abc"}
        self._send_request(params=params)
        self._after(self)
