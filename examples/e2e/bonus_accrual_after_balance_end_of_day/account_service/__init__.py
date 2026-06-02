import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import account_service.db_model as db_model
import config
from helpers import data_generator, HttpSession
from helpers.quartz import QuartzBaseDB

service_name = 'AccountService'
requests_session = HttpSession()

requests_session.url = f'https://{config.demo_api_host}/service/accountservice/api/'
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

    def delete_clients_by_external_user_ref(self, external_user_ref):
        clients = self.session.query(self.model.Client).filter_by(ExternalUserRef=external_user_ref).all()
        for client in clients:
            self.session.delete(client)
        self.session.commit()

    def delete_clients_by_demo_user_refs(self):
        """
        Удаление клиентов по номерам из ответа мока ContractControl.getContract
        """
        clients = self.session.query(self.model.Client)\
            .filter(self.model.Client.ExternalUserRef.in_(config.demo_user_refs)).all()
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
