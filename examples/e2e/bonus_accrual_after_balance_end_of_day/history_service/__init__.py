import urllib.parse

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import config
import history_service.db_model as db_model
from helpers import HttpSession
from helpers.quartz import QuartzBaseDB

service_name = 'HistoryService'

requests_session = HttpSession()
requests_session.url = f'https://{config.k8s_host}/service/historyservice/api/'
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

    def _query_qr_precheck_by_partner_order_id(self, partner_order_id: int):
        return self.session.query(self.model.QrPrecheck).filter_by(PartnerOrderId=partner_order_id)

    def get_qr_precheck(self, partner_order_id: int) -> db_model.QrPrecheck | None:
        """Получение записи QrPrecheck по PartnerOrderId."""
        self.session.expire_all()
        return self._query_qr_precheck_by_partner_order_id(partner_order_id).one_or_none()

    def get_qr_prechecks(self, partner_order_id: int) -> list[db_model.QrPrecheck]:
        """Получение всех записей QrPrecheck по PartnerOrderId."""
        self.session.expire_all()
        return self._query_qr_precheck_by_partner_order_id(partner_order_id).order_by(self.model.QrPrecheck.Id).all()

    def count_qr_precheck(self, partner_order_id: int) -> int:
        """Подсчет количества записей QrPrecheck по PartnerOrderId."""
        self.session.expire_all()
        return self._query_qr_precheck_by_partner_order_id(partner_order_id).count()

    def add_qr_precheck(self, topic) -> db_model.QrPrecheck:
        """Добавление записи QrPrecheck из сообщения топика."""
        record = self.model.QrPrecheck()
        record.set_value(topic)
        self.session.add(record)
        self.session.commit()
        self.session.refresh(record)
        return record

    def delete_qr_precheck(self, partner_order_id: int) -> None:
        """Удаление всех записей QrPrecheck по PartnerOrderId."""
        records = self._query_qr_precheck_by_partner_order_id(partner_order_id).all()
        for record in records:
            self.session.delete(record)
        self.session.commit()
