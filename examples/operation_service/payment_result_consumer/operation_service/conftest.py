import allure
import pytest

from operation_service import Db as DbOperationService
from account_service import Db as DbAccountService


@allure.title('Инициализация сессии БД AccountService для создания клиентов')
@pytest.fixture(scope='session')
def db_account_service():
    db = DbAccountService()
    yield db
    db.session.invalidate()
    db.session.close()


@allure.title('Инициализация сессии БД OperationService для проверки операций')
@pytest.fixture(scope='session')
def db():
    db = DbOperationService()
    yield db
    db.session.invalidate()
    db.session.close()


@allure.title('Очистка сессий БД')
@pytest.fixture(scope='class', autouse=True)
def clear_db_session(db, db_account_service):
    db.session.close()
    db_account_service.session.close()
