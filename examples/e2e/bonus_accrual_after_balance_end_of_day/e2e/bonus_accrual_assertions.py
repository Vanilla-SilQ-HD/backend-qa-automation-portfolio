from datetime import UTC

from hamcrest import assert_that, equal_to, greater_than, none, not_none

from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_accounts_transactions import PortfolioAccountsTransactionsTopic
from kafka_topics.internal.portfolio_bonus_operation import PortfolioBonusOperationTopic

from e2e.bonus_accrual_flow import (
    BONUS_ACCOUNT_TYPE_ID,
    BONUS_CURRENCY_ID,
    BONUS_OPERATION_EVENT_TYPE_ID,
    CLIENT_ID,
    TRANSACTION_DESCRIPTION,
    TRANSACTION_EVENT_ID,
    TRANSACTION_STATUS_ID,
    TRANSACTION_TYPE_ID,
    normalize_db_event_time_to_utc,
    parse_event_time,
)


class BonusAccrualAssertions:
    def __init__(self, context, flow):
        self.context = context
        self.flow = flow

    def assert_bonus_operation_event(self):
        message = self.flow.wait_bonus_message()
        topic = PortfolioBonusOperationTopic()
        with allure.step('Проверяем сообщение через topic checker'):
            topic.check_accrual_for_balance_message(
                message=message,
                expected_client_id=CLIENT_ID,
                expected_source_account_id=self.context.source_account_id,
                expected_event_type_id=BONUS_OPERATION_EVENT_TYPE_ID,
            )

    def assert_bonus_row(self):
        c = self.context
        self.flow.wait_bonus_message()
        bonus_packages = c.bonus_message.value['bonusOperation']['bonusPackages']
        assert_that(len(bonus_packages), greater_than(0), 'bonusPackages должен содержать пакет')

        with allure.step('Проверяем запись в BonusService.Bonuses'):
            bonus_row = self._get_bonus_row()
            bonus_rule = self._get_bonus_rule(bonus_row.RuleId)

            assert_that(str(bonus_row.ClientId), equal_to(CLIENT_ID), 'Некорректный ClientId в Bonuses')
            assert_that(str(bonus_row.AccountId), equal_to(c.source_account_id), 'Некорректный AccountId в Bonuses')
            assert_that(bonus_row.Amount, equal_to(c.amount), 'Некорректный Amount в Bonuses')
            assert_that(bonus_row.CurrentBalance, equal_to(c.amount), 'Некорректный CurrentBalance в Bonuses')
            assert_that(bonus_row.OperationAmount, equal_to(c.balance_amount), 'Некорректный OperationAmount в Bonuses')

            self._assert_bonus_package(bonus_packages[0], bonus_row, bonus_rule)
            assert_that(
                parse_event_time(c.bonus_message.value['bonusOperation']['operationDate']).astimezone(UTC),
                equal_to(normalize_db_event_time_to_utc(bonus_row.OperationDate)),
                'Некорректный operationDate в bonusOperation относительно Bonuses.OperationDate',
            )

    def _get_bonus_row(self):
        c = self.context
        bonus_row = (
            c.bonus_db.session.query(c.bonus_db.model.Bonuses)
            .filter(c.bonus_db.model.Bonuses.AccountId == c.source_account_id)
            .one_or_none()
        )
        assert_that(bonus_row, not_none(), 'Запись в Bonuses не найдена')
        allure_attach(bonus_row, 'bonus_row')
        return bonus_row

    def _get_bonus_rule(self, rule_id):
        c = self.context
        bonus_rule = (
            c.bonus_db.session.query(c.bonus_db.model.AccrualRule)
            .filter(c.bonus_db.model.AccrualRule.RuleId == rule_id)
            .one_or_none()
        )
        assert_that(bonus_rule, not_none(), f'AccrualRule не найдено по RuleId={rule_id}')
        allure_attach(bonus_rule, 'bonus_accrual_rule')
        return bonus_rule

    def _assert_bonus_package(self, package, bonus_row, bonus_rule):
        c = self.context
        assert_that(package['bonusId'], equal_to(bonus_row.Id), 'Некорректный bonusPackages[0].bonusId')
        assert_that(package['amountUsed'], equal_to(c.amount), 'Некорректный bonusPackages[0].amountUsed')
        if 'currency' in package:
            assert_that(package['currency'], equal_to(bonus_rule.CurrencyId), 'Некорректный bonusPackages[0].currency')
        if 'percentage' in package:
            assert_that(package['percentage'], equal_to(bonus_rule.Percentage), 'Некорректный bonusPackages[0].percentage')
        if 'status' in package:
            assert_that(package['status'], equal_to(bonus_row.StatusId))

    def assert_transaction_event(self):
        c = self.context
        message = self.flow.wait_transaction_message()
        topic = PortfolioAccountsTransactionsTopic()
        with allure.step('Проверяем сообщение через topic checker'):
            topic.check_message(
                message=message,
                expected_account_id=c.bonus_account_id,
                expected_client_id=CLIENT_ID,
                expected_event_time=c.event_time,
                expected_account_type_id=BONUS_ACCOUNT_TYPE_ID,
                expected_currency=BONUS_CURRENCY_ID,
                expected_amount=c.amount,
                expected_type_id=TRANSACTION_TYPE_ID,
                expected_event_id=TRANSACTION_EVENT_ID,
                expected_status_id=TRANSACTION_STATUS_ID,
                expected_description=TRANSACTION_DESCRIPTION,
            )

    def assert_transaction_row(self):
        c = self.context
        self.flow.wait_transaction_message()
        with allure.step('Проверяем запись в AccountService.BalanceTransactions'):
            c.account_db.session.expire_all()
            tx_row = self._get_transaction_row()
            allure_attach(tx_row, 'balance_transaction_row')
            assert_that(str(tx_row.ClientId), equal_to(CLIENT_ID), 'Некорректный ClientId в BalanceTransactions')
            assert_that(str(tx_row.AccountId), equal_to(c.bonus_account_id), 'Некорректный AccountId в BalanceTransactions')
            assert_that(tx_row.Amount, equal_to(c.amount), 'Некорректный Amount в BalanceTransactions')
            assert_that(tx_row.CurrencyId, equal_to(BONUS_CURRENCY_ID), 'Некорректный CurrencyId в BalanceTransactions')
            assert_that(tx_row.TypeId, equal_to(TRANSACTION_TYPE_ID), 'Некорректный TypeId в BalanceTransactions')
            assert_that(tx_row.EventId, equal_to(TRANSACTION_EVENT_ID), 'Некорректный EventId в BalanceTransactions')
            assert_that(tx_row.StatusId, equal_to(TRANSACTION_STATUS_ID), 'Некорректный StatusId в BalanceTransactions')
            assert_that(tx_row.Description, equal_to(TRANSACTION_DESCRIPTION), 'Некорректный Description в BalanceTransactions')
            assert_that(tx_row.OperationId, none(), 'OperationId должен быть null в BalanceTransactions')
            assert_that(
                normalize_db_event_time_to_utc(tx_row.EventTime),
                equal_to(parse_event_time(c.event_time).astimezone(UTC)),
                'Некорректный EventTime в BalanceTransactions',
            )

    def _get_transaction_row(self):
        c = self.context
        tx_row = None
        if c.tx_id is not None:
            tx_row = (
                c.account_db.session.query(c.account_db.model.BalanceTransaction)
                .filter(c.account_db.model.BalanceTransaction.Id == int(c.tx_id))
                .one_or_none()
            )
        if tx_row is None:
            tx_row = self.flow.find_transaction_row(c.amount)
        assert_that(tx_row, not_none(), 'Запись BalanceTransactions не найдена')
        return tx_row

    def assert_bonus_balance(self):
        c = self.context
        self.flow.wait_bonus_message()
        with allure.step('Проверяем обновление AccountService.Balances'):
            balance_row = (
                c.account_db.session.query(c.account_db.model.Balance)
                .filter(c.account_db.model.Balance.AccountId == c.bonus_account_id)
                .one()
            )
            allure_attach(balance_row, 'balance_row_after')
            assert_that(balance_row.ActiveBalance, equal_to(c.before_active), 'Некорректный ActiveBalance после начисления')
            assert_that(balance_row.FrozenBalance, equal_to(c.before_frozen + c.amount), 'Некорректный FrozenBalance после начисления')
