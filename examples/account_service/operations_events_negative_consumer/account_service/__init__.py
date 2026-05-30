import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import account_service.db_model
import config
from helpers import data_generator, HttpSession
from helpers.quartz import QuartzBaseDB

db_model = account_service.db_model

service_name = 'AccountService'
requests_session = HttpSession()

requests_session.url = f'https://{config.k8s_host}/service/accountservice/api/'
requests_session.verify = False
requests_session.headers.update({'Content-Type': 'application/json'})


class CustomSession(Session):
    pass


class Db(QuartzBaseDB):
    """
    Класс для работы с БД
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
        database = 'Portfolio_AccountService'
        connection_string = f'{dialect}://{username}:{password}@{host}:{port}/{database}'
        self.engine = create_engine(connection_string, echo=False, pool_pre_ping=True)
        self.session = CustomSession(self.engine, autocommit=False, autoflush=False, expire_on_commit=False)
        self.model = db_model

    def _add_client(self, client_from_db) -> db_model.Client:
        self.session.add(client_from_db)
        self.session.commit()
        self.session.refresh(client_from_db)
        return client_from_db

    def add_temporary_client(self, client: data_generator.Client) -> db_model.Client:
        client_from_db = self.model.Client()
        client_from_db.set_temporary(client)
        return self._add_client(client_from_db)

    def add_anonymous_client(self, client: data_generator.Client) -> db_model.Client:
        client_from_db = self.model.Client()
        client_from_db.set_anonymous(client)
        return self._add_client(client_from_db)

    def add_anonymous_with_client_type_client(self, client: data_generator.Client) -> db_model.Client:
        client_from_db = self.model.Client()
        client_from_db.set_anonymous_with_client_type(client)
        return self._add_client(client_from_db)

    def add_active_client(self, client: data_generator.Client) -> db_model.Client:
        client_from_db = self.model.Client()
        client_from_db.set_active(client)
        return self._add_client(client_from_db)

    def add_identified_client(self, client: data_generator.Client) -> db_model.Client:
        """Добавление идентифицированного клиента с LevelId=10 (для операций СБП)"""
        client_from_db = self.model.Client()
        client_from_db.set_identified(client)
        return self._add_client(client_from_db)

    def _add_balance(self, balance) -> db_model.Balance:
        self.session.add(balance)
        self.session.commit()
        self.session.refresh(balance)
        return balance

    def add_bonus_client(self, client: data_generator.Client, balance_amount: int) -> db_model.Client:
        """Добавление клиента с бонусным счётом и активным балансом (для бонусных операций)"""
        client_from_db = self.model.Client()
        client_from_db.set_bonus(client)
        client_from_db = self._add_client(client_from_db)
        balance = self.model.Balance()
        balance.set_values(
            client_id=client.id,
            account_id=client.one_account.id,
            active_balance=balance_amount,
        )
        self._add_balance(balance)
        self.session.refresh(client_from_db)
        return client_from_db

    def add_balance_transaction(self, client_id, account_id, operation_id, amount, currency_id,
                                type_id, event_id, description, status_id: int = 1) -> db_model.BalanceTransaction:
        """Прямой сид BalanceTransaction в БД AccountService (как preconditions теста)"""
        balance_transaction = self.model.BalanceTransaction()
        balance_transaction.set_values(
            client_id=client_id,
            account_id=account_id,
            operation_id=operation_id,
            amount=amount,
            currency_id=currency_id,
            type_id=type_id,
            event_id=event_id,
            description=description,
            status_id=status_id,
        )
        self.session.add(balance_transaction)
        self.session.commit()
        self.session.refresh(balance_transaction)
        return balance_transaction

    def delete_clients_by_msisdn(self, msisdn):
        clients = self.session.query(self.model.Client).filter_by(Msisdn=msisdn).all()
        for client in clients:
            self.session.delete(client)
        self.session.commit()

    def delete_clients_by_contract_msisdns(self):
        """
        Удаление клиентов по номерам из ответа мока ContractControl.getContract
        """
        clients = self.session.query(self.model.Client)\
            .filter(self.model.Client.Msisdn.in_(config.contract_msisdns)).all()
        for client in clients:
            self.session.delete(client)
        self.session.commit()

    def _add_account(self, account_form_db) -> db_model.Account:
        self.session.add(account_form_db)
        self.session.commit()
        self.session.refresh(account_form_db)
        return account_form_db

    def add_savings_account(self, client_id: str):
        """
        Добавление счета TypeId=2, StatusId=20
        """
        account_from_db = self.model.Account()
        account_from_db.set_saving(client_id)
        return self._add_account(account_from_db)
