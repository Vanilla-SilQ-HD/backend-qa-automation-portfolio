from datetime import UTC, datetime, timedelta, timezone

from hamcrest import assert_that, equal_to, greater_than, not_none

import config
from helpers.allure_client import allure, allure_attach


CLIENT_ID = '11111111-1111-1111-1111-111111111111'
SOURCE_ACCOUNT_TYPE_ID = 2
BONUS_ACCOUNT_TYPE_ID = 3
SOURCE_CURRENCY_ID = 643
BONUS_CURRENCY_ID = 9991
ACCOUNT_STATUS_ACTIVE = 20
TRANSACTION_TYPE_ID = 1
TRANSACTION_EVENT_ID = 7771
BONUS_OPERATION_EVENT_TYPE_ID = 1
TRANSACTION_STATUS_ID = 1
TRANSACTION_DESCRIPTION = 'Начисление бонусов'
MSK_UTC_PLUS_3 = timezone(timedelta(hours=3))


def parse_event_time(value: str) -> datetime:
    if value.endswith('Z'):
        value = value[:-1] + '+00:00'
    return datetime.fromisoformat(value)


def normalize_db_event_time_to_utc(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(UTC)
    return value.replace(tzinfo=MSK_UTC_PLUS_3).astimezone(UTC)


class BonusAccrualFlow:
    def __init__(self, context):
        self.context = context

    def prepare_source_account(self):
        c = self.context
        m = c.account_db.model
        now = datetime.now(UTC).replace(microsecond=0)

        client_exists = c.account_db.session.query(m.Client.Id).filter(
            m.Client.Id == CLIENT_ID
        ).one_or_none()
        assert_that(client_exists, not_none(), f'В БД нет клиента {CLIENT_ID}')

        bonus_account = self._get_bonus_account()
        c.bonus_account_id = bonus_account.Id
        allure_attach(bonus_account, 'bonus_account')

        balance_row = self._get_bonus_balance_row()
        allure_attach(balance_row, 'balance_row_before')
        c.before_active = balance_row.ActiveBalance
        c.before_frozen = balance_row.FrozenBalance

        self._assert_active_accrual_rules(now)
        self._create_source_account(now)

    def _get_bonus_account(self):
        c = self.context
        m = c.account_db.model
        bonus_accounts = (
            c.account_db.session.query(m.Account)
            .filter(
                m.Account.ClientId == CLIENT_ID,
                m.Account.TypeId == BONUS_ACCOUNT_TYPE_ID,
                m.Account.CurrencyId == BONUS_CURRENCY_ID,
                m.Account.StatusId == ACCOUNT_STATUS_ACTIVE,
            )
            .all()
        )
        assert_that(
            len(bonus_accounts),
            equal_to(1),
            f'Ожидался ровно один бонусный счет клиента {CLIENT_ID}, найдено: {len(bonus_accounts)}',
        )
        return bonus_accounts[0]

    def _get_bonus_balance_row(self):
        c = self.context
        balance_row = (
            c.account_db.session.query(c.account_db.model.Balance)
            .filter(c.account_db.model.Balance.AccountId == c.bonus_account_id)
            .one_or_none()
        )
        assert_that(balance_row, not_none(), f'Нет строки Balances для бонусного счета {c.bonus_account_id}')
        return balance_row

    def _assert_active_accrual_rules(self, now):
        c = self.context
        bm = c.bonus_db.model
        accrual_rules = (
            c.bonus_db.session.query(bm.AccrualRule)
            .filter(
                bm.AccrualRule.AccountTypeId == SOURCE_ACCOUNT_TYPE_ID,
                bm.AccrualRule.ValidFrom < now,
                bm.AccrualRule.ValidTo > now,
            )
            .all()
        )
        allure_attach(
            {'count': len(accrual_rules), 'rules': [self._rule_to_dict(rule) for rule in accrual_rules]},
            'active_accrual_rules_for_source_account_type',
        )
        assert_that(
            len(accrual_rules),
            greater_than(0),
            'В BonusService не найдено активное AccrualRule для AccountTypeId=2',
        )

    @staticmethod
    def _rule_to_dict(rule):
        return {
            'RuleId': rule.RuleId,
            'AccountTypeId': rule.AccountTypeId,
            'Percentage': rule.Percentage,
            'MinAmountForBonus': rule.MinAmountForBonus,
            'CurrencyId': rule.CurrencyId,
            'BonusStatusId': rule.BonusStatusId,
            'ValidFrom': rule.ValidFrom.isoformat() if rule.ValidFrom is not None else None,
            'ValidTo': rule.ValidTo.isoformat() if rule.ValidTo is not None else None,
        }

    def _create_source_account(self, now):
        c = self.context
        source_account = c.account_db.model.Account()
        source_account.Id = c.source_account_id
        source_account.ClientId = CLIENT_ID
        source_account.TypeId = SOURCE_ACCOUNT_TYPE_ID
        source_account.StatusId = ACCOUNT_STATUS_ACTIVE
        source_account.CurrencyId = SOURCE_CURRENCY_ID
        source_account.CreatedOn = now
        source_account.CreatedBy = 'srv.account'
        source_account.ModifiedOn = now
        source_account.ModifiedBy = 'srv.account'
        source_account = c.account_db._add_account(source_account)
        allure_attach(source_account, 'source_account')

    def wait_bonus_message(self):
        c = self.context
        if c.bonus_message is not None:
            return c.bonus_message

        with allure.step(f'Ожидаем сообщение в {config.bonus_operation_topic}'):
            search_criteria = {'bonusOperation': {'accountId': c.source_account_id}}
            allure_attach(
                {'topic': config.bonus_operation_topic, 'search_criteria': search_criteria},
                'bonus_operation_wait_search_context',
            )
            c.bonus_message = c.kafka.wait_message(
                topic_name=config.bonus_operation_topic,
                search_criteria=search_criteria,
                timeout=120,
                time_shift=600,
            )
            assert_that(c.bonus_message, not_none(), 'Сообщение Portfolio.Bonus.Operation не найдено')
            allure_attach(c.bonus_message, 'bonus_operation_message')

        c.amount = c.bonus_message.value['bonusOperation']['amount']
        self.attach_bonus_row_snapshot()
        return c.bonus_message

    def wait_transaction_message(self):
        c = self.context
        if c.tx_message is not None:
            return c.tx_message

        self.wait_bonus_message()
        with allure.step(f'Ожидаем сообщение в {config.accounts_transactions_topic}'):
            c.tx_message = c.kafka.wait_message(
                topic_name=config.accounts_transactions_topic,
                search_criteria={
                    'accountId': c.bonus_account_id,
                    'clientId': CLIENT_ID,
                    'eventTime': c.event_time,
                },
                timeout=120,
                time_shift=600,
            )
            assert_that(c.tx_message, not_none(), 'Сообщение Portfolio.Accounts.Transactions не найдено')
            allure_attach(c.tx_message, 'accounts_transactions_message')

        c.tx_id = c.tx_message.value.get('transactionId')
        return c.tx_message

    def attach_bonus_row_snapshot(self):
        c = self.context
        c.bonus_db.session.expire_all()
        bonus_row = (
            c.bonus_db.session.query(c.bonus_db.model.Bonuses)
            .filter(c.bonus_db.model.Bonuses.AccountId == c.source_account_id)
            .one_or_none()
        )
        if bonus_row is None:
            allure_attach(
                {
                    'clientId': CLIENT_ID,
                    'source_account_id': c.source_account_id,
                    'bonus_row_found': False,
                },
                'bonus_row_after_balance_end_of_day',
            )
            return

        allure_attach(
            {
                'bonus_row_found': True,
                'Id': bonus_row.Id,
                'ClientId': str(bonus_row.ClientId),
                'AccountId': str(bonus_row.AccountId),
                'RuleId': bonus_row.RuleId,
                'StatusId': bonus_row.StatusId,
                'Amount': bonus_row.Amount,
                'CurrentBalance': bonus_row.CurrentBalance,
                'OperationAmount': bonus_row.OperationAmount,
                'OperationDate': bonus_row.OperationDate.isoformat() if bonus_row.OperationDate is not None else None,
                'CreatedOn': bonus_row.CreatedOn.isoformat() if bonus_row.CreatedOn is not None else None,
                'ModifiedOn': bonus_row.ModifiedOn.isoformat() if bonus_row.ModifiedOn is not None else None,
            },
            'bonus_row_after_balance_end_of_day',
        )

    def find_transaction_row(self, amount: int):
        c = self.context
        m = c.account_db.model
        expected_utc = parse_event_time(c.event_time).astimezone(UTC)
        rows = (
            c.account_db.session.query(m.BalanceTransaction)
            .filter(
                m.BalanceTransaction.ClientId == CLIENT_ID,
                m.BalanceTransaction.AccountId == c.bonus_account_id,
                m.BalanceTransaction.Amount == amount,
                m.BalanceTransaction.CurrencyId == BONUS_CURRENCY_ID,
                m.BalanceTransaction.TypeId == TRANSACTION_TYPE_ID,
                m.BalanceTransaction.EventId == TRANSACTION_EVENT_ID,
                m.BalanceTransaction.StatusId == TRANSACTION_STATUS_ID,
                m.BalanceTransaction.Description == TRANSACTION_DESCRIPTION,
            )
            .all()
        )
        for row in rows:
            if normalize_db_event_time_to_utc(row.EventTime) == expected_utc:
                return row
        return None

    def cleanup(self):
        c = self.context
        am = c.account_db.model
        bm = c.bonus_db.model
        c.account_db.session.rollback()
        c.bonus_db.session.rollback()

        if c.tx_id is not None:
            (
                c.account_db.session.query(am.BalanceTransaction)
                .filter(am.BalanceTransaction.Id == c.tx_id)
                .delete(synchronize_session=False)
            )
        elif c.amount is not None:
            tx_row = self.find_transaction_row(c.amount)
            if tx_row is not None:
                (
                    c.account_db.session.query(am.BalanceTransaction)
                    .filter(am.BalanceTransaction.Id == tx_row.Id)
                    .delete(synchronize_session=False)
                )

        (
            c.account_db.session.query(am.Account)
            .filter(am.Account.Id == c.source_account_id)
            .delete(synchronize_session=False)
        )

        balance_row = (
            c.account_db.session.query(am.Balance)
            .filter(am.Balance.AccountId == c.bonus_account_id)
            .one_or_none()
        )
        if balance_row is not None:
            balance_row.ActiveBalance = c.before_active
            balance_row.FrozenBalance = c.before_frozen
        c.account_db.session.commit()

        (
            c.bonus_db.session.query(bm.Bonuses)
            .filter(bm.Bonuses.AccountId == c.source_account_id)
            .delete(synchronize_session=False)
        )
        c.bonus_db.session.commit()
