import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import bonus_service.db_model as db_model
import config
from helpers import HttpSession, datetime_now

service_name = 'BonusService'

requests_session = HttpSession()
requests_session.url = f'https://{config.demo_api_host}/service/bonusservice/api/'
requests_session.verify = False
requests_session.headers.update({'Content-Type': 'application/json', 'accept': 'application/json'})


class CustomSession(Session):
    pass


class Db:
    """
    Класс для работы с БД BonusService
    """

    def __init__(self):
        """
        Создание коннекта и сессии
        """
        dialect = 'postgresql'
        username = config.db_user
        password = urllib.parse.quote_plus(config.db_password)
        host = config.db_host
        port = 5432
        database = 'Portfolio_BonusService'
        connection_string = f'{dialect}://{username}:{password}@{host}:{port}/{database}'
        self.engine = create_engine(connection_string, echo=False, pool_pre_ping=True)
        self.session = CustomSession(self.engine, autocommit=False, autoflush=False, expire_on_commit=False)
        self.model = db_model

    def get_active_accrual_rule(self, currency_id: int) -> db_model.AccrualRule:
        """Действующее правило начисления бонусов для валюты"""
        now = datetime_now()
        return self.session.query(self.model.AccrualRule)\
            .filter(self.model.AccrualRule.CurrencyId == currency_id,
                    self.model.AccrualRule.ValidFrom < now,
                    self.model.AccrualRule.ValidTo > now)\
            .order_by(self.model.AccrualRule.RuleId).first()

    def add_bonus(self, client_id, account_id, rule_id, amount, expired_date) -> db_model.Bonuses:
        """Добавление активного бонусного пакета"""
        bonus = self.model.Bonuses()
        bonus.set_values(
            client_id=client_id,
            account_id=account_id,
            rule_id=rule_id,
            amount=amount,
            expired_date=expired_date,
        )
        self.session.add(bonus)
        self.session.commit()
        self.session.refresh(bonus)
        return bonus
