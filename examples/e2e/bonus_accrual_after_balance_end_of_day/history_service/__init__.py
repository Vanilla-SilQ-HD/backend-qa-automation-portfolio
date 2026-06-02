import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import config
import history_service.db_model as db_model
from helpers import HttpSession
from helpers.quartz import QuartzBaseDB

service_name = 'HistoryService'

requests_session = HttpSession()
requests_session.url = f'https://{config.demo_api_host}/service/historyservice/api/'
requests_session.verify = False
requests_session.headers.update(
    {
        'Content-Type': 'application/json',
        'accept': 'application/json',
    }
)


class CustomSession(Session):
    pass


class Db(QuartzBaseDB):
    """
    Класс для работы с БД.
    """

    def __init__(self):
        """
        Создание коннекта и сессии.
        """
        dialect = 'postgresql'
        username = config.db_user
        password = urllib.parse.quote_plus(config.db_password)
        host = config.db_host
        port = 5432
        database = 'Portfolio_HistoryService'
        connection_string = f'{dialect}://{username}:{password}@{host}:{port}/{database}'
        self.engine = create_engine(connection_string, echo=False, pool_pre_ping=True)
        self.session = CustomSession(self.engine, autocommit=False, autoflush=False, expire_on_commit=False)
        self.model = db_model
