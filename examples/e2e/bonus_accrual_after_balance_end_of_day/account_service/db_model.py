import uuid
from copy import deepcopy
from datetime import timedelta, datetime
from typing import List, Optional

from hamcrest import assert_that, equal_to, none, not_none
from sqlalchemy import BigInteger, DateTime, ForeignKeyConstraint, Identity, Integer, PrimaryKeyConstraint, String, UUID, text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from helpers import data_generator, datetime_now, check_assert_that
from helpers.db_base_model import Base


class Account(Base):
    __tablename__ = 'Accounts'
    __table_args__ = (
        ForeignKeyConstraint(['ClientId'], ['Clients.Id'], ondelete='CASCADE', name='FK_Accounts_Clients_ClientId'),
        ForeignKeyConstraint(['CreatedBy'], ['Systems.Id'], ondelete='RESTRICT', name='FK_Accounts_Systems_CreatedBy'),
        ForeignKeyConstraint(['ModifiedBy'], ['Systems.Id'], ondelete='RESTRICT', name='FK_Accounts_Systems_ModifiedBy'),
        ForeignKeyConstraint(['StatusId'], ['AccountStatuses.Id'], ondelete='RESTRICT', name='FK_Accounts_AccountStatuses_StatusId'),
        ForeignKeyConstraint(['TypeId'], ['AccountTypes.Id'], ondelete='RESTRICT', name='FK_Accounts_AccountTypes_TypeId'),
        PrimaryKeyConstraint('Id', name='PK_Accounts')
    )

    Id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    ClientId: Mapped[str] = mapped_column(UUID(as_uuid=False))
    TypeId: Mapped[int] = mapped_column(Integer)
    StatusId: Mapped[int] = mapped_column(Integer)
    CurrencyId: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedBy: Mapped[str] = mapped_column(String(64))
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))
    ModifiedBy: Mapped[str] = mapped_column(String(64))
    MaskedPan: Mapped[Optional[str]] = mapped_column(String(16))

    client: Mapped['Client'] = relationship('Client', back_populates='accounts')
    created_system: Mapped['System'] = relationship('System', foreign_keys=[CreatedBy], viewonly=True)
    modified_system: Mapped['System'] = relationship('System', foreign_keys=[ModifiedBy], viewonly=True)
    status: Mapped['AccountStatus'] = relationship('AccountStatus', viewonly=True, lazy='joined')
    account_type: Mapped['AccountType'] = relationship('AccountType', viewonly=True, lazy='joined')
    status_journals: Mapped[List['StatusJournal']] = relationship(
        'StatusJournal', back_populates='account',
        order_by='StatusJournal.Id', cascade='all, delete-orphan'
    )

    def __init__(self):
        self.CreatedBy = 'srv.account'
        self.ModifiedBy = 'srv.account'
        self.status_journals = []

    def check_process(self, client: data_generator.Client):
        check_assert_that(self.ClientId, equal_to(client.id), 'Ошибка в ClientId')
        check_assert_that(self.TypeId, equal_to(1), 'Ошибка в TypeId')  # Кошелек
        check_assert_that(self.StatusId, equal_to(10), 'Ошибка в StatusId')  # В процессе оформления
        check_assert_that(self.MaskedPan, none(), 'Ошибка в MaskedPan')
        check_assert_that(abs(self.CreatedOn - datetime_now()) < timedelta(minutes=5), 'Ошибка в CreatedOn')
        check_assert_that(self.CreatedBy, equal_to('srv.account'), 'Ошибка в CreatedBy')
        check_assert_that(abs(self.ModifiedOn - datetime_now()) < timedelta(minutes=5), 'Ошибка в ModifiedOn')
        check_assert_that(self.ModifiedBy, equal_to('srv.account'), 'Ошибка в ModifiedBy')
        check_assert_that(self.status_journals, equal_to([]), 'Ошибка в status_journals')

    def set_process(self, client: data_generator.Client):
        self.CreatedOn = client.one_account.created_on
        self.Id = client.one_account.id
        self.ClientId = client.id
        self.TypeId = 1  # Кошелек
        self.StatusId = 10  # В процессе оформления
        self.ModifiedOn = client.one_account.modified_on
        self.check_process(client)

    def check_new(self, account_before, client: data_generator.Client):
        check_assert_that(self.StatusId, equal_to(15), 'Ошибка в StatusId')  # Новый(Неактивный)
        check_assert_that(
            abs(self.ModifiedOn - datetime_now()) < timedelta(minutes=5),
            'Ошибка в ModifiedOn'
        )
        check_assert_that(self.ModifiedBy, equal_to('srv.account'), 'Ошибка в ModifiedBy')
        check_assert_that(self.MaskedPan, equal_to(client.pan), 'Ошибка в MaskedPan')
        check_assert_that(
            self.is_equal(account_before, ignored=['StatusId', 'ModifiedOn', 'ModifiedBy', 'MaskedPan']),
            'Некорректное изменение записи'
        )
        check_assert_that(len(self.status_journals), equal_to(1), 'Ошибка в status_journals')
        self.status_journals[0].check(status_id=self.StatusId)

    def set_new(self, client: data_generator.Client):
        self.set_process(client)
        account_before = deepcopy(self)
        self.StatusId = 15  # Новый(Неактивный)
        self.ModifiedOn = client.one_account.modified_on
        self.ModifiedBy = 'srv.account'
        self.MaskedPan = client.pan
        status_journal = StatusJournal()
        status_journal.set(status_id=self.StatusId)
        self.status_journals.append(status_journal)
        self.check_new(account_before, client)

    def check_active(self, account_before, client: data_generator.Client):
        check_assert_that(self.StatusId, equal_to(20), 'Ошибка в StatusId')  # Активный
        check_assert_that(
            abs(self.ModifiedOn - datetime_now()) < timedelta(minutes=5),
            'Ошибка в ModifiedOn'
        )
        check_assert_that(self.ModifiedBy, equal_to('srv.account'), 'Ошибка в ModifiedBy')
        check_assert_that(
            self.is_equal(account_before, ignored=['StatusId', 'ModifiedOn', 'ModifiedBy']),
            'Некорректное изменение записи'
        )
        check_assert_that(len(self.status_journals), equal_to(2), 'Ошибка в status_journals')
        self.status_journals[1].check(self.StatusId)

    def set_active(self, client: data_generator.Client):
        self.set_new(client)
        account_before = deepcopy(self)
        self.StatusId = 20  # Активный
        self.ModifiedOn = client.one_account.modified_on
        self.ModifiedBy = 'srv.account'
        status_journal = StatusJournal()
        status_journal.set(status_id=self.StatusId)
        self.status_journals.append(status_journal)
        self.check_active(account_before, client)

    def set_saving(self, client_id: str):
        self.Id = str(uuid.uuid4())
        self.ClientId = client_id
        self.TypeId = 2
        self.StatusId = 20
        self.CreatedOn = datetime_now()
        self.ModifiedOn = datetime_now()
        self.MaskedPan = None


class Balance(Base):
    __tablename__ = 'Balances'
    __table_args__ = (
        ForeignKeyConstraint(['AccountId'], ['Accounts.Id'], ondelete='CASCADE', name='FK_Balances_Accounts_AccountId'),
        PrimaryKeyConstraint('AccountId', name='PK_Balances'),
    )

    AccountId: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    ClientId: Mapped[str] = mapped_column(UUID(as_uuid=False))
    ActiveBalance: Mapped[int] = mapped_column(BigInteger)
    FrozenBalance: Mapped[int] = mapped_column(BigInteger)
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedBy: Mapped[str] = mapped_column(String(64))
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))
    ModifiedBy: Mapped[str] = mapped_column(String(64))


class BalanceTransaction(Base):
    __tablename__ = 'BalanceTransactions'
    __table_args__ = (
        ForeignKeyConstraint(['AccountId'], ['Accounts.Id'], ondelete='CASCADE', name='FK_BalanceTransactions_Accounts_AccountId'),
        PrimaryKeyConstraint('Id', name='PK_BalanceTransactions'),
    )

    Id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ClientId: Mapped[str] = mapped_column(UUID(as_uuid=False))
    AccountId: Mapped[str] = mapped_column(UUID(as_uuid=False))
    OperationId: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    EventTime: Mapped[datetime] = mapped_column(DateTime(True))
    Amount: Mapped[int] = mapped_column(BigInteger)
    CurrencyId: Mapped[int] = mapped_column(Integer)
    TypeId: Mapped[int] = mapped_column(Integer)
    EventId: Mapped[int] = mapped_column(Integer)
    StatusId: Mapped[int] = mapped_column(Integer)
    Description: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedBy: Mapped[str] = mapped_column(String(64))


class AccountStatus(Base):
    __tablename__ = 'AccountStatuses'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_AccountStatuses'),
    )

    Id: Mapped[int] = mapped_column(Integer, primary_key=True)
    Name: Mapped[str] = mapped_column(String(50))


class AccountType(Base):
    __tablename__ = 'AccountTypes'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_AccountTypes'),
    )

    Id: Mapped[int] = mapped_column(Integer, primary_key=True)
    Name: Mapped[str] = mapped_column(String(50))


class ClientAccountLevel(Base):
    __tablename__ = 'ClientAccountLevels'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_ClientAccountLevels'),
    )

    Id: Mapped[int] = mapped_column(Integer, primary_key=True)
    Name: Mapped[str] = mapped_column(String(50))


class ClientIdPassphrase(Base):
    __tablename__ = 'ClientIdPassphrase'
    __table_args__ = (
        ForeignKeyConstraint(['ClientId'], ['Clients.Id'], ondelete='CASCADE', name='FK_ClientIdPassphrase_Clients_ClientId'),
        PrimaryKeyConstraint('ClientId', name='PK_ClientIdPassphrase')
    )

    ClientId: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    CurrentPassphrase: Mapped[Optional[str]] = mapped_column(String(100))
    NewPassphrase: Mapped[Optional[str]] = mapped_column(String(100))

    client: Mapped['Client'] = relationship('Client', back_populates='passphrase')

    def __init__(self):
        self.CurrentPassphrase = '3MTTsa7IZxXdnrUEYEp57VVbjIvtkiNBfenfWKiqWqvwbRnss73ZrpKWnOUgqg=='


class Client(Base):
    __tablename__ = 'Clients'
    __table_args__ = (
        ForeignKeyConstraint(['CreatedBy'], ['Systems.Id'], ondelete='RESTRICT', name='FK_Clients_Systems_CreatedBy'),
        ForeignKeyConstraint(['LevelId'], ['ClientAccountLevels.Id'], ondelete='SET NULL', name='FK_Clients_ClientAccountLevels_LevelId'),
        ForeignKeyConstraint(['StatusId'], ['ClientStatuses.Id'], ondelete='SET NULL', name='FK_Clients_ClientStatuses_StatusId'),
        PrimaryKeyConstraint('Id', name='PK_Clients')
    )

    Id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    Msisdn: Mapped[str] = mapped_column(String(11))
    BranchId: Mapped[int] = mapped_column(Integer)
    ClientAccountId: Mapped[int] = mapped_column(Integer)
    StatusId: Mapped[int] = mapped_column(Integer)
    LevelId: Mapped[int] = mapped_column(Integer, server_default=text('0'))
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedBy: Mapped[str] = mapped_column(String(64), server_default=text("''::character varying"))
    SubsId: Mapped[int] = mapped_column(Integer, server_default=text('0'))
    ClientType: Mapped[Optional[str]] = mapped_column(String(10))
    FailedAttempt: Mapped[Optional[int]] = mapped_column(Integer)
    BlockedUntil: Mapped[Optional[datetime]] = mapped_column(DateTime(True))

    system: Mapped['System'] = relationship('System', viewonly=True)
    account_level: Mapped['ClientAccountLevel'] = relationship('ClientAccountLevel', viewonly=True, lazy='joined')
    status: Mapped['ClientStatus'] = relationship('ClientStatus', viewonly=True, lazy='joined')
    accounts: Mapped[List['Account']] = relationship('Account', back_populates='client',
                                                     lazy='joined', cascade='all, delete-orphan')
    passphrase: Mapped['ClientIdPassphrase'] = relationship('ClientIdPassphrase', back_populates='client',
                                                            uselist=False, lazy='joined', cascade='all, delete-orphan')

    def dict(self):
        if self.passphrase:
            passphrase = self.passphrase.dict()
        else:
            passphrase = dict()
        client = super().dict()
        client.update({'passphrase': passphrase})
        return client

    def __eq__(self, other, ignored=()):
        if 'passphrase' in ignored or (self.passphrase is None and other.passphrase is None):
            return super().__eq__(other, ignored)
        else:
            return super().__eq__(other, ignored) and self.passphrase.is_equal(other.passphrase, ignored)

    @property
    def one_account(self) -> Account:
        assert_that(len(self.accounts), equal_to(1), 'Некорректное количество accounts')
        return self.accounts[0]

    @property
    def last_account(self) -> Account:
        if len(self.accounts) == 0:
            return None
        return max(self.accounts, key=lambda x: x.CreatedOn)

    def __init__(self):
        self.CreatedOn = datetime_now()
        self.CreatedBy = 'srv.account'
        self.accounts = []

    def check_temporary(self, client: data_generator.Client):
        check_assert_that(self.Msisdn, equal_to(client.msisdn), 'Ошибка в Msisdn')
        check_assert_that(self.BranchId, equal_to(client.branch_id), 'Ошибка в BranchId')
        check_assert_that(self.ClientAccountId, equal_to(client.esb_client_id), 'Ошибка в ClientAccountId')
        check_assert_that(self.StatusId, equal_to(10), 'Ошибка в StatusId')  # Активный
        check_assert_that(self.LevelId, equal_to(2), 'Ошибка в LevelId')  # Временный
        check_assert_that(self.ClientType, none(), 'Ошибка в ClientType')
        check_assert_that(self.FailedAttempt, equal_to(0), 'Ошибка в FailedAttempt')
        check_assert_that(self.BlockedUntil, none(), 'Ошибка в BlockedUntil')
        check_assert_that(abs(self.CreatedOn - datetime_now()) < timedelta(minutes=5), 'Ошибка в CreatedOn')
        check_assert_that(self.CreatedBy, equal_to('srv.account'), 'Ошибка в CreatedBy')
        check_assert_that(self.SubsId, equal_to(client.esb_subs_id), 'Ошибка в SubsId')
        check_assert_that(self.accounts, equal_to([]), 'Ошибка в accounts')
        check_assert_that(self.passphrase, none(), 'Ошибка в passphrase')

    def set_temporary(self, client: data_generator.Client):
        self.Id = client.id
        self.Msisdn = client.msisdn
        self.BranchId = client.branch_id
        self.ClientAccountId = client.esb_client_id
        self.StatusId = 10  # Активный
        self.LevelId = 2  # Временный
        self.SubsId = client.esb_subs_id
        self.ClientType = None
        self.FailedAttempt = 0
        self.BlockedUntil = None
        self.passphrase = None
        self.check_temporary(client)

    def check_anonymous(self, client_before):
        check_assert_that(self.LevelId, equal_to(5), 'Ошибка в LevelId')  # Анонимный
        check_assert_that(abs(self.CreatedOn - datetime_now()) < timedelta(minutes=5),
                          'Ошибка в CreatedOn')
        check_assert_that(
            self.is_equal(client_before, ignored=['LevelId', 'CreatedOn', 'passphrase']),
            'Некорректное изменение записи'
        )
        passphrase = self.passphrase
        assert_that(passphrase, not_none(), 'Запись отсутствует запись в ClientIdPassphrase')
        # Placeholder field kept for parity with the source model.
        check_assert_that(passphrase.CurrentPassphrase, not_none(), 'Ошибка в CurrentPassphrase')
        check_assert_that(passphrase.NewPassphrase, none(), 'Ошибка в NewPassphrase')

    def set_anonymous(self, client: data_generator.Client):
        self.set_temporary(client)
        client_before = deepcopy(self)
        self.LevelId = 5  # Анонимный
        self.passphrase = ClientIdPassphrase()
        self.check_anonymous(client_before)
        account = Account()
        account.set_process(client)
        self.accounts.append(account)

    def check_anonymous_with_client_type(self, client_before, client_type):
        check_assert_that(self.ClientType, equal_to(client_type), 'Ошибка в ClientType')
        check_assert_that(
            self.is_equal(client_before, ignored=['ClientType']),
            'Некорректное изменение записи'
        )

    def set_anonymous_with_client_type(self, client: data_generator.Client):
        self.set_anonymous(client)
        client_before = deepcopy(self)
        self.ClientType = client.client_type
        self.check_anonymous_with_client_type(client_before, client_type=client.client_type)
        account = Account()
        account.set_new(client)
        self.accounts[0] = account

    def set_active(self, client: data_generator.Client):
        self.set_anonymous_with_client_type(client)
        account = Account()
        account.set_active(client)
        self.accounts[0] = account

    def set_identified(self, client: data_generator.Client):
        """Создание идентифицированного клиента с LevelId=10 (необходим для операций СБП)"""
        self.set_anonymous_with_client_type(client)
        client_before = deepcopy(self)
        self.LevelId = 10  # Идентифицированный
        account = Account()
        account.set_active(client)
        self.accounts[0] = account


class ClientStatus(Base):
    __tablename__ = 'ClientStatuses'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_ClientStatuses'),
    )

    Id: Mapped[int] = mapped_column(Integer, primary_key=True)
    Name: Mapped[str] = mapped_column(String(50))


class ClientServices(Base):
    __tablename__ = 'ClientServices'
    __table_args__ = (
        ForeignKeyConstraint(['ClientId'], ['public.Clients.Id'], ondelete='CASCADE', name='FK_ClientServices_Clients_ClientId'),
        ForeignKeyConstraint(['ServiceId'], ['public.Services.Id'], name='FK_ClientServices_Services_ServiceId'),
        ForeignKeyConstraint(['StatusId'], ['public.ClientServiceStatuses.Id'], ondelete='RESTRICT', name='FK_ClientServices_ClientServiceStatuses_StatusId'),
        PrimaryKeyConstraint('Id', name='PK_ClientServices'),
        {'schema': 'public'}
    )

    Id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    ClientId: Mapped[uuid.UUID] = mapped_column(Uuid)
    ServiceId: Mapped[int] = mapped_column(Integer)
    StatusId: Mapped[int] = mapped_column(Integer)
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    ModifiedOn: Mapped[datetime] = mapped_column(DateTime(True))


class StatusJournal(Base):
    __tablename__ = 'StatusJournal'
    __table_args__ = (
        ForeignKeyConstraint(['AccountId'], ['Accounts.Id'], ondelete='CASCADE', name='FK_StatusJournal_Accounts_AccountId'),
        ForeignKeyConstraint(['CreatedBy'], ['Systems.Id'], ondelete='RESTRICT', name='FK_StatusJournal_Systems_CreatedBy'),
        ForeignKeyConstraint(['NewStatusId'], ['AccountStatuses.Id'], ondelete='RESTRICT', name='FK_StatusJournal_AccountStatuses_NewStatusId'),
        ForeignKeyConstraint(['StatusModifiedBy'], ['Systems.Id'], ondelete='RESTRICT', name='FK_StatusJournal_Systems_StatusModifiedBy'),
        PrimaryKeyConstraint('Id', name='PK_StatusJournal')
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1, minvalue=1, maxvalue=2147483647, cycle=False, cache=1), primary_key=True)
    AccountId: Mapped[str] = mapped_column(UUID(as_uuid=False))
    NewStatusId: Mapped[int] = mapped_column(Integer)
    CreatedOn: Mapped[datetime] = mapped_column(DateTime(True))
    CreatedBy: Mapped[str] = mapped_column(String(64))
    StatusModifiedBy: Mapped[Optional[str]] = mapped_column(String(64))
    StatusModifiedOn: Mapped[Optional[datetime]] = mapped_column(DateTime(True))

    account: Mapped['Account'] = relationship('Account', back_populates='status_journals')
    created_system: Mapped['System'] = relationship('System', foreign_keys=[CreatedBy], viewonly=True)
    account_status: Mapped['AccountStatus'] = relationship('AccountStatus', viewonly=True)
    modified_system: Mapped[Optional['System']] = relationship('System', foreign_keys=[StatusModifiedBy], viewonly=True)

    def __init__(self):
        self.CreatedOn = datetime_now()
        self.CreatedBy = 'srv.account'
        self.StatusModifiedBy = 'srv.account'
        self.StatusModifiedOn = datetime_now()

    def check(self, status_id):
        check_assert_that(self.NewStatusId, equal_to(status_id), 'Ошибка в NewStatusId')
        check_assert_that(self.StatusModifiedBy, equal_to('srv.account'), 'Ошибка в StatusModifiedBy')
        check_assert_that(
            abs(self.StatusModifiedOn - datetime_now()) < timedelta(minutes=5),
            'Ошибка в StatusModifiedOn'
        )
        check_assert_that(
            abs(self.CreatedOn - datetime_now()) < timedelta(minutes=5),
            'Ошибка в CreatedOn'
        )
        check_assert_that(self.CreatedBy, equal_to('srv.account'), 'Ошибка в CreatedBy')

    def set(self, status_id):
        self.NewStatusId = status_id


class System(Base):
    __tablename__ = 'Systems'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_Systems'),
    )

    Id: Mapped[str] = mapped_column(String(64), primary_key=True)
    Name: Mapped[str] = mapped_column(String(255))
