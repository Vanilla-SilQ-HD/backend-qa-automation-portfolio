import allure
import pytest

from bonus_service import Db


@allure.title('Инициализация сессии БД BonusService')
@pytest.fixture(scope='session')
def db():
    db = Db()
    yield db
    db.session.invalidate()
    db.session.close()


@allure.title('Очистка сессии БД BonusService')
@pytest.fixture(scope='class', autouse=True)
def clear_db_session(db):
    db.session.close()
