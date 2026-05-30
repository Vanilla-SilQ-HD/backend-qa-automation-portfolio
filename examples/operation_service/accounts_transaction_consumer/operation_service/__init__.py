import urllib.parse
from datetime import datetime, timezone

import config
from helpers import HttpSession, data_generator
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import operation_service.db_model as db_model

service_name = "OperationService"
requests_session = HttpSession()

requests_session.url = f"https://{config.k8s_host}/service/operationsservice/api/"
requests_session.verify = False
requests_session.headers.update({"Content-Type": "application/json"})


class CustomSession(Session):
    pass


class Db:
    """
    Класс для работы с БД OperationsService
    """

    def __init__(self):
        """
        Создание коннекта и сессии
        """
        dialect = "postgresql"
        username = config.db_user
        password = urllib.parse.quote_plus(config.db_password)
        host = config.db_host
        port = 5432
        database = "Portfolio_OperationsService"
        connection_string = (
            f"{dialect}://{username}:{password}@{host}:{port}/{database}"
        )
        self.engine = create_engine(connection_string, echo=False, pool_pre_ping=True)
        self.session = CustomSession(
            self.engine, autocommit=False, autoflush=False, expire_on_commit=False
        )
        self.model = db_model

    def add_clients_accounts_data(self, client) -> db_model.ClientsAccountsData:
        """
        Статусы уровня клиента (LevelId):
        - 0: обнулен
        - 2: временный
        - 5: анонимный
        - 10: именной
        - 15: идентифицированный
        """
        record = db_model.ClientsAccountsData()
        record.set_value(client)
        self.session.add(record)
        self.session.commit()
        self.session.refresh(record)
        return record

    def add_operation(self, operation):
        """
        Добавление операции в БД из бизнес-сущности data_generator.Operation

        Args:
            operation: экземпляр data_generator.Operation

        Returns:
            db_model.Operation: созданная операция в БД
        """
        # Получаем справочники
        operation_state = (
            self.session.query(db_model.OperationState)
            .filter_by(Name=operation.operation_state_name)
            .one()
        )

        operation_type = (
            self.session.query(db_model.OperationType)
            .filter_by(Name=operation.operation_type_name)
            .one()
        )

        # Создаем SQLAlchemy модель
        operation_from_db = db_model.Operation(
            ClientId=operation.client_id,
            AccountId=operation.account_id,
            OperationStateId=operation_state.Id,
            OperationTypeId=operation_type.Id,
            Amount=operation.amount,
            Currency=operation.currency,
            Fee=operation.fee,
            Description=operation.description,
            CreatedOn=operation.created_on,
            ModifiedOn=operation.modified_on,
        )

        self.session.add(operation_from_db)
        self.session.flush()

        # Добавляем SbpData, если есть
        if operation.sbp_data:
            operation_from_db.sbp_data = db_model.SbpData(
                OperationId=operation_from_db.Id,
                RecipientFIO=operation.sbp_data.get("recipient_fio"),
                SbpBankId=operation.sbp_data.get("sbp_bank_id"),
                Phone=operation.sbp_data.get("phone"),
                SbpTranId=operation.sbp_data.get("sbp_tran_id"),
                CreatedOn=operation.created_on,
                ModifiedOn=operation.modified_on,
            )

        self.session.commit()
        self.session.refresh(operation_from_db)
        return operation_from_db

    def get_active_qr_directories(self) -> list[db_model.QrDirectories]:
        return (
            self.session.query(db_model.QrDirectories)
            .filter_by(IsActive=True)
            .order_by(db_model.QrDirectories.Id)
            .all()
        )
