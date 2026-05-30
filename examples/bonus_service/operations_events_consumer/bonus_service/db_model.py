from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Integer, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from helpers import datetime_now
from helpers.db_base_model import Base


class RuleType(Base):
    __tablename__ = 'RuleTypes'
    __table_args__ = (PrimaryKeyConstraint('Id', name='PK_RuleTypes'),)

    Id: Mapped[int] = mapped_column(Integer, primary_key=True)
    Name: Mapped[str] = mapped_column(String(50))


class AccrualRule(Base):
    __tablename__ = 'AccrualRules'
    __table_args__ = (PrimaryKeyConstraint('RuleId', name='PK_AccrualRules'),)

    RuleId: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    RuleTypeId: Mapped[int] = mapped_column(ForeignKey('RuleTypes.Id'))
    AccountTypeId: Mapped[int] = mapped_column(Integer)
    Percentage: Mapped[float] = mapped_column(Float)
    OperationCategoryId: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    MinAmountForBonus: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    MaxAmountForBonus: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    CurrencyId: Mapped[int] = mapped_column(Integer)
    BonusStatusId: Mapped[int] = mapped_column(Integer)
    BurningPeriodMonths: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    BurningPeriodDay: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ActivationMonth: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ActivationDay: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ValidTo: Mapped[datetime] = mapped_column(DateTime(True))
    ValidFrom: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedBy: Mapped[str] = mapped_column(String(64))
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))
    ModifiedBy: Mapped[str] = mapped_column(String(64))

    RuleType: Mapped['RuleType'] = relationship('RuleType')
    bonuses: Mapped[list['Bonuses']] = relationship('Bonuses', back_populates='rule')


class Bonuses(Base):
    __tablename__ = 'Bonuses'
    __table_args__ = (PrimaryKeyConstraint('Id', name='PK_Bonuses'),)

    Id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ClientId: Mapped[str] = mapped_column(String(64))
    AccountId: Mapped[str] = mapped_column(String(64))
    RuleId: Mapped[int] = mapped_column(ForeignKey('AccrualRules.RuleId'))
    StatusId: Mapped[int] = mapped_column(Integer)
    OperationId: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    OperationDate: Mapped[datetime] = mapped_column(DateTime(True))
    Amount: Mapped[int] = mapped_column(BigInteger)
    CurrentBalance: Mapped[int] = mapped_column(BigInteger)
    OperationAmount: Mapped[int] = mapped_column(BigInteger)
    ActivationDate: Mapped[datetime] = mapped_column(DateTime(True))
    ExpiredDate: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedBy: Mapped[str] = mapped_column(String(64))
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))
    ModifiedBy: Mapped[str] = mapped_column(String(64))

    rule: Mapped['AccrualRule'] = relationship('AccrualRule', back_populates='bonuses')

    def set_values(self, client_id, account_id, rule_id, amount, expired_date,
                   status_id=1, operation_id=None):
        """Активный бонусный пакет (StatusId=1 ACTIVE, CurrentBalance=Amount)"""
        now = datetime_now()
        self.ClientId = client_id
        self.AccountId = account_id
        self.RuleId = rule_id
        self.StatusId = status_id
        self.OperationId = operation_id
        self.OperationDate = now
        self.Amount = amount
        self.CurrentBalance = amount
        self.OperationAmount = amount
        self.ActivationDate = now - timedelta(hours=1)
        self.ExpiredDate = expired_date
        self.CreatedOn = now
        self.CreatedBy = 'Portfolio.Autotests'
        self.ModifiedOn = now
        self.ModifiedBy = 'Portfolio.Autotests'


class WithdrawallOperation(Base):
    __tablename__ = " WithdrawallOperations"

    Id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ClientId: Mapped[str] = mapped_column(String(64))
    OperationId: Mapped[int] = mapped_column(BigInteger)
    OperationDate: Mapped[datetime] = mapped_column(DateTime(True))
    OperationTypeId: Mapped[int] = mapped_column(Integer)
    Amount: Mapped[int] = mapped_column(BigInteger)
    WithdrawalStatusId: Mapped[int] = mapped_column(Integer)
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedBy: Mapped[str] = mapped_column(String(64))
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))
    ModifiedBy: Mapped[str] = mapped_column(String(64))


class WithdrawallOperationDetail(Base):
    __tablename__ = "WithdrawallOperationDetails"

    Id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    WithdrawallId: Mapped[int] = mapped_column(BigInteger)
    WithdrawallOperationId: Mapped[int] = mapped_column(BigInteger)
    BonusId: Mapped[int] = mapped_column(BigInteger, ForeignKey("Bonuses.Id"))
    AmountUsed: Mapped[int] = mapped_column(BigInteger)
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedBy: Mapped[str] = mapped_column(String(64))
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))
    ModifiedBy: Mapped[str] = mapped_column(String(64))


__all__ = [
    "RuleType",
    "AccrualRule",
    "Bonuses",
    "WithdrawallOperation",
    "WithdrawallOperationDetail",
]
