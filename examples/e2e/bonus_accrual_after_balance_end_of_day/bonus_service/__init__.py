import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import bonus_service.db_model as db_model
import config
from helpers import HttpSession

service_name = 'BonusService'

requests_session = HttpSession()
requests_session.url = f'https://{config.k8s_host}/service/bonusservice/api/'
requests_session.verify = False
requests_session.headers.update(
    {
        'Content-Type': 'application/json',
        'accept': 'application/json',
    }
)


class CustomSession(Session):
    pass


class Db:
    """
    Класс для работы с БД BonusService.
    """

    def __init__(self) -> None:
        """
        Создание коннекта и сессии.
        """
        dialect = 'postgresql'
        username = config.db_user
        password = urllib.parse.quote_plus(config.db_password)
        host = config.db_host
        port = 5432
        database = 'Portfolio_BonusService'
        connection_string = f'{dialect}://{username}:{password}@{host}:{port}/{database}'
        self.engine = create_engine(
            connection_string,
            echo=False,
            pool_pre_ping=True,
        )
        self.session = CustomSession(
            self.engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
        self.model = db_model
