import allure
import psutil
import pytest
from xdist.scheduler.loadscope import LoadScopeScheduling


default_environment = 'dev'

# ниже парсинг параметров запуска (из-за дочерних потоков воркеров xdist)
process = psutil.Process()
while process:
    environment = next((a.split('=')[-1] for a in process.cmdline() if a.startswith('--env=')), None)
    if environment:
        break
    process = process.parent()
else:
    environment = default_environment


def pytest_runtest_setup(item):
    """
    Убирает дубли allure-маркеров от наследования
    """
    marks = set()
    for mark in reversed(list(item.iter_markers(name='allure_label'))):
        label_type = mark.kwargs['label_type']
        if label_type.lstrip('_') not in ['epic', 'feature', 'story']:
            continue
        if label_type.lstrip('_') in marks:
            mark.kwargs['label_type'] = f'_{label_type}'
        else:
            mark.kwargs['label_type'] = label_type.lstrip('_')
            marks.add(label_type.lstrip('_'))


def pytest_addoption(parser):
    """
    Прокидываем параметры из командной строки в тесты
    """
    parser.addoption('--env', default=default_environment,
                     help=f'test, test-02, dev, stage, ect or aft. By default: {default_environment}', action='store')


def pytest_configure(config):
    """
    Определяем глобальные переменные по параметрам командной строки
    """
    global environment
    environment = config.getoption('--env').lower()
    assert environment in ['test', 'test-02', 'dev', 'stage', 'ect', 'aft'], f'Unknown environment: "{environment}"'


def pytest_sessionstart(session):
    """
    Операции до запуска тестов
    """
    session.results = dict()


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    result = outcome.get_result()
    if result.when == 'call':
        item.session.results[item] = result


def pytest_sessionfinish(session, exitstatus):
    """
    Операции после запуска тестов
    """
    from helpers import set_test_report
    if not hasattr(session.config, 'workerinput'):
        from helpers import allure_client
        allure_client.add_environment()
        # allure_client.upload_results_testops()
        # allure_client.upload_results()
        set_test_report(session, exitstatus)


class CustomXdistScheduler(LoadScopeScheduling):
    """
    Тесты распараллеливаются по сервисам (папкам). Все неперечисленные тесты также запускаются последовательно
    """

    def _split_scope(self, nodeid):
        if nodeid.split('/')[0] in ['account_service', 'payment_provider_service']:
            return 'thread_1'
        elif nodeid.split('/')[0] in ['e2e']:
            return 'thread_2'
        else:
            return 'thread_3'


def pytest_xdist_make_scheduler(config, log):
    return CustomXdistScheduler(config, log)


@allure.title('Инициализация клиента для внутренней Kafka')
@pytest.fixture(scope='session')
def kafka():
    from helpers.kafka_client import Kafka
    kafka = Kafka(kind='internal')
    yield kafka
    kafka.consumer.close(autocommit=False)
    kafka.producer.close()


@allure.title('Инициализация клиента для внешней Kafka')
@pytest.fixture(scope='session')
def external_kafka():
    from helpers.kafka_client import Kafka
    kafka = Kafka(kind='external')
    yield kafka
    kafka.consumer.close(autocommit=False)
    kafka.producer.close()

@allure.title("Инициализация клиента для Redis")
@pytest.fixture(scope='session')
def redis():
    from helpers.redis_client import Redis
    redis_client = Redis()
    yield redis_client



@allure.title('Добавление пароля для подписи PaymentProvider в мок-сервис')
@pytest.fixture(scope='session', autouse=True)
def add_payment_provider_password():
    from helpers.mock_service import send_tempdata
    from config import payment_provider_password
    send_tempdata(key='payment_provider_password', value=payment_provider_password)


@allure.title('Создание клиента в БД AccountService')
@pytest.fixture(scope='class', params=[
        'process',
        'new',
        'active',
        'deleted account',
        'disabled account',
        'banned',
    ])
def add_client(request):
    from account_service import Db
    from helpers.allure_client import allure, allure_attach
    from helpers import data_generator
    with allure.step('Генерируем клиента'):
        client = data_generator.Client()
        allure_attach(client, 'client')
    with allure.step('Добавляем клиента в БД'):
        db = Db()
        if request.param == 'temporary':
            client_from_db = db.add_temporary_client(client)
        elif request.param == 'process':
            client_from_db = db.add_anonymous_client(client)
            assert client_from_db.one_account.StatusId == 10  # В процессе оформления
        elif request.param == 'new':
            client_from_db = db.add_anonymous_with_client_type_client(client)
            assert client_from_db.one_account.StatusId == 15  # Новый(Неактивный)
        elif request.param == 'active':
            client_from_db = db.add_active_client(client)
            assert client_from_db.one_account.StatusId == 20  # Активный
        elif request.param == 'deleted account':
            client_from_db = db.add_active_client(client)
            client_from_db.one_account.StatusId = 30  # Закрытый
        elif request.param == 'disabled account':
            client_from_db = db.add_active_client(client)
            client_from_db.one_account.StatusId = 25  # Заблокированный
        elif request.param == 'banned':
            client_from_db = db.add_active_client(client)
            client_from_db.StatusId = 20  # Заблокированный
            client_from_db.one_account.StatusId = 25  # Заблокированный
        db.session.commit()
        db.session.refresh(client_from_db)
        allure_attach(client_from_db, 'client_from_db')
        allure_attach(client_from_db.accounts, 'accounts_from_db')
        db.session.expunge_all()
    db.session.invalidate()
    db.session.close()
    return client, client_from_db


@allure.title('Инициализация сессии БД')
@pytest.fixture(scope='session')
def account_service_db():
    from account_service import Db
    db = Db()
    yield db
    db.session.invalidate()
    db.session.close()
