import allure
import pytest

from account_service import Db


@allure.title('Инициализация сессии БД')
@pytest.fixture(scope='session')
def db():
    db = Db()
    yield db
    db.session.invalidate()
    db.session.close()


@allure.title('Очистка сессии БД')
@pytest.fixture(scope='class', autouse=True)
def clear_db_session(db):
    db.session.close()
