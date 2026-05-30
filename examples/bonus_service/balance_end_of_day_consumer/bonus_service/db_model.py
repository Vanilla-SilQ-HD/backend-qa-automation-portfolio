from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Integer, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

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

    def set_values(
        self,
        client_id: str,
        account_id: str,
        rule_id: int,
        status_id: int,
        operation_id: Optional[int],
        operation_date: datetime,
        amount: int,
        current_balance: int,
        operation_amount: int,
        activation_date: datetime,
        expired_date: datetime,
        created_on: datetime,
        created_by: str,
        modified_on: datetime,
        modified_by: str,
        bonus_id: Optional[int] = None,
    ):
        if bonus_id is not None:
            self.Id = bonus_id
        self.ClientId = client_id
        self.AccountId = account_id
        self.RuleId = rule_id
        self.StatusId = status_id
        self.OperationId = operation_id
        self.OperationDate = operation_date
        self.Amount = amount
        self.CurrentBalance = current_balance
        self.OperationAmount = operation_amount
        self.ActivationDate = activation_date
        self.ExpiredDate = expired_date
        self.CreatedOn = created_on
        self.CreatedBy = created_by
        self.ModifiedOn = modified_on
        self.ModifiedBy = modified_by


__all__ = [
    'RuleType',
    'AccrualRule',
    'Bonuses',
]

