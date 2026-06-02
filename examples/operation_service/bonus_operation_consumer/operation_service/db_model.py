from datetime import datetime, timezone
from typing import Optional, List
import uuid

from sqlalchemy import BigInteger, DateTime, Float, ForeignKeyConstraint, Index, Integer, PrimaryKeyConstraint, \
    String, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from helpers.db_base_model import Base


class Operation(Base):
    __tablename__ = 'Operations'
    __table_args__ = (
        ForeignKeyConstraint(['OperationStateId'], ['OperationState.Id'], name='FK_Operations_OperationState'),
        ForeignKeyConstraint(['OperationTypeId'], ['OperationType.Id'], name='FK_Operations_OperationType'),
        PrimaryKeyConstraint('Id', name='PK_Operations'),
        Index('IX_Operations_ClientId', 'ClientId'),
        Index('IX_Operations_ModifiedOn', 'ModifiedOn'),
    )

    Id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    AccountId: Mapped[Optional[str]] = mapped_column(UUID(as_uuid=False))
    ClientId: Mapped[str] = mapped_column(UUID(as_uuid=False))
    ExtId: Mapped[Optional[str]] = mapped_column(String)
    SdRef: Mapped[Optional[str]] = mapped_column(String)
    OperationStateId: Mapped[int] = mapped_column(BigInteger)
    SectorId: Mapped[Optional[int]] = mapped_column(BigInteger)
    OperationTypeId: Mapped[Optional[int]] = mapped_column(BigInteger)
    PartnerOperationId: Mapped[Optional[int]] = mapped_column(BigInteger)
    TwoId: Mapped[Optional[str]] = mapped_column(String)
    PartnerOperationDate: Mapped[Optional[datetime]] = mapped_column(DateTime(True))
    PartnerOrderId: Mapped[Optional[int]] = mapped_column(BigInteger)
    PartnerOrderDate: Mapped[Optional[datetime]] = mapped_column(DateTime(True))
    Amount: Mapped[Optional[int]] = mapped_column(BigInteger)
    Fee: Mapped[Optional[int]] = mapped_column(BigInteger)
    Currency: Mapped[Optional[int]] = mapped_column(BigInteger)
    Description: Mapped[Optional[str]] = mapped_column(String(1000))
    ReasonCodeId: Mapped[Optional[int]] = mapped_column(BigInteger)
    Message: Mapped[Optional[str]] = mapped_column(String)
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))

    operation_state: Mapped['OperationState'] = relationship('OperationState', viewonly=True)
    operation_type: Mapped[Optional['OperationType']] = relationship('OperationType', viewonly=True)


class ClientsAccountsData(Base):
    """
    Таблица ClientsAccountsData - денормализованная таблица с информацией о счетах и клиентах.

    Содержит данные из account_service для быстрого доступа.
    """
    __tablename__ = 'ClientsAccountsData'
    __table_args__ = (
        PrimaryKeyConstraint('AccountId', name='PK_ClientsAccountsData'),
    )

    AccountId: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    ClientId: Mapped[str] = mapped_column(UUID(as_uuid=False))
    ExternalUserRef: Mapped[str] = mapped_column(String(11))
    ClientStatusId: Mapped[int] = mapped_column(Integer)
    AccountStatusId: Mapped[int] = mapped_column(Integer)
    AccountTypeId: Mapped[int] = mapped_column(Integer)
    CurrencyId: Mapped[Optional[int]] = mapped_column(Integer)
    LevelId: Mapped[int] = mapped_column(Integer)
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))

    def set_value(self, client):
        self.AccountId = client.one_account.id
        self.ClientId = client.id
        self.ExternalUserRef = client.external_user_ref
        self.ClientStatusId = 10
        self.AccountStatusId = 20
        self.AccountTypeId = 3
        self.CurrencyId = 9991
        self.LevelId = 10
        self.ModifiedOn = datetime.now(timezone.utc)


class OperationState(Base):
    """
    Справочник OperationState - статусы операций.

    Возможные значения:
    - 10: Created - операция создана
    - 11: CheckBalance - проверка баланса
    - 12: BonusOperation - бонусная операция
    - 15: Pending - ожидает обработки
    - 17: CheckSuccess - проверка успешна
    - 20: Success - операция успешна
    - 25: Error - ошибка выполнения операции
    """
    __tablename__ = 'OperationState'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_OperationState'),
    )

    Id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    Name: Mapped[str] = mapped_column(String)


class OperationType(Base):
    """
    Справочник OperationType - типы операций.

    Возможные значения:
    - 0: Authorize - авторизация
    - 5: Complete - завершение
    - 10: Purchase - покупка
    - 15: Reverse - отмена операции
    - 26: RewardCreditBonus - synthetic reward credit operation
    - 45: ME2METRANSFER - перевод ME2ME
    - 9999: Unknown - неизвестный тип операции
    """
    __tablename__ = 'OperationType'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_OperationType'),
    )

    Id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    Name: Mapped[str] = mapped_column(String)
    SectorId: Mapped[Optional[int]] = mapped_column(BigInteger)
    FeeProcent: Mapped[Optional[float]] = mapped_column(Float)
    LifePeriod: Mapped[Optional[str]] = mapped_column(String(500))
