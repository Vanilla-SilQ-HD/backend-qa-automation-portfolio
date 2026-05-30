from datetime import datetime, timedelta
from typing import Optional

from hamcrest import equal_to
from sqlalchemy import BigInteger, Boolean, DateTime, Integer, PrimaryKeyConstraint, String, UUID

from helpers import check_assert_that, datetime_now
from helpers.db_base_model import Base
from sqlalchemy.orm import Mapped, mapped_column


def _parse_topic_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


class QrPrecheck(Base):
    __tablename__ = 'QrPrecheck'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_QrPrecheck'),
    )

    Id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    PartnerOrderId: Mapped[int] = mapped_column(BigInteger)
    PartnerOrderDate: Mapped[datetime] = mapped_column(DateTime(True))
    PrecheckId: Mapped[str] = mapped_column(UUID(as_uuid=False))
    ClientId: Mapped[str] = mapped_column(UUID(as_uuid=False))
    Amount: Mapped[Optional[int]] = mapped_column(BigInteger)
    Currency: Mapped[Optional[int]] = mapped_column(Integer)
    IsOpenAmmount: Mapped[Optional[bool]] = mapped_column(Boolean)
    MerchantReference: Mapped[Optional[str]] = mapped_column(UUID(as_uuid=False))
    QrClass: Mapped[Optional[str]] = mapped_column(String)
    QrType: Mapped[Optional[str]] = mapped_column(String)
    QrScenario: Mapped[Optional[str]] = mapped_column(String)
    MerchantId: Mapped[Optional[str]] = mapped_column(String)
    MerchantName: Mapped[Optional[str]] = mapped_column(String)
    Mcc: Mapped[Optional[str]] = mapped_column(String)
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))

    def set_value(self, topic):
        self.PartnerOrderId = topic._partner_order_id
        self.PartnerOrderDate = _parse_topic_datetime(topic._partner_order_date)
        self.PrecheckId = topic._precheck_id
        self.ClientId = topic._client.id
        self.Amount = topic._payment.amount
        self.Currency = topic._payment.currency
        self.IsOpenAmmount = topic._payment.isOpenAmount
        self.MerchantReference = topic._reference
        self.QrClass = topic._qr.class_
        self.QrType = topic._qr.type
        self.QrScenario = topic._qr.scenario
        self.MerchantId = topic._merchant.id
        self.MerchantName = topic._merchant.name
        self.Mcc = topic._merchant.mcc
        self.CreatedOn = datetime_now()
        self.ModifiedOn = datetime_now()

    def check(self, topic):
        check_assert_that(self.PartnerOrderId, equal_to(topic._partner_order_id), 'Ошибка в PartnerOrderId')
        check_assert_that(
            self.PartnerOrderDate,
            equal_to(_parse_topic_datetime(topic._partner_order_date)),
            'Ошибка в PartnerOrderDate',
        )
        check_assert_that(self.PrecheckId, equal_to(topic._precheck_id), 'Ошибка в PrecheckId')
        check_assert_that(self.ClientId, equal_to(topic._client.id), 'Ошибка в ClientId')
        check_assert_that(self.Amount, equal_to(topic._payment.amount), 'Ошибка в Amount')
        check_assert_that(self.Currency, equal_to(topic._payment.currency), 'Ошибка в Currency')
        check_assert_that(self.IsOpenAmmount, equal_to(topic._payment.isOpenAmount), 'Ошибка в IsOpenAmmount')
        check_assert_that(self.MerchantReference, equal_to(topic._reference), 'Ошибка в MerchantReference')
        check_assert_that(self.QrClass, equal_to(topic._qr.class_), 'Ошибка в QrClass')
        check_assert_that(self.QrType, equal_to(topic._qr.type), 'Ошибка в QrType')
        check_assert_that(self.QrScenario, equal_to(topic._qr.scenario), 'Ошибка в QrScenario')
        check_assert_that(self.MerchantId, equal_to(topic._merchant.id), 'Ошибка в MerchantId')
        check_assert_that(self.MerchantName, equal_to(topic._merchant.name), 'Ошибка в MerchantName')
        check_assert_that(self.Mcc, equal_to(topic._merchant.mcc), 'Ошибка в Mcc')
        check_assert_that(
            abs(self.CreatedOn - datetime_now()) < timedelta(minutes=5),
            'Ошибка в CreatedOn',
        )
        check_assert_that(
            abs(self.ModifiedOn - datetime_now()) < timedelta(minutes=5),
            'Ошибка в ModifiedOn',
        )
