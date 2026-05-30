"""
Сценарии на базе типового колбэка POST .../v1/callback/paymentStatus из public API contract.
"""

import pytest
from hamcrest import assert_that, equal_to

from helpers import data_generator
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_payments_result import PortfolioPaymentsResultTopic
from payment_gateway_adapter_service import operation_payment_status_xml, requests_session, service_name


pytestmark = [
    allure.epic(service_name),
    allure.feature('POST /api/v1/callback/paymentStatus'),
    allure.label('microservice', service_name),
    allure.link(
        'https://example.com/spec-redacted',
        name='Specification',
    ),
    allure.tag(service_name),
    allure.tag('API'),
    allure.tag('callback'),
]


class _CallbackPaymentStatusBase:
    expected_operation_state_id = None
    payment_result_attach_name = 'payment_result_message'

    @allure.title('Подготовка данных для тестов')
    @pytest.fixture(scope='class', autouse=True)
    def prepare(self, request, kafka):
        self = request.cls
        self.kafka = kafka
        self._before(self)
        self._send_request(self)
        self._after(self)

    def _before(self):
        self.client = data_generator.Client()
        allure_attach(self.client, 'client')
        self.body = operation_payment_status_xml(
            self.client.id,
            self.terminal_id,
            state=self.state,
            reason_code=self.reason_code,
            message=self.message,
        )
        self.url = f'{requests_session.url}v1/callback/paymentStatus'

    def _send_request(self):
        self.response = requests_session.request(
            requests_session,
            'POST',
            self.url,
            data=self.body.strip(),
            headers={'Content-Type': 'application/xml'},
        )

    def _after(self):
        with allure.step('Проверка HTTP-ответа'):
            assert_that(self.response.status_code, equal_to(200), 'Некорректный статус ответа')
            assert_that(self.response.text.strip(), equal_to('ok'), 'Ошибка в теле ответа')

        self.topic = PortfolioPaymentsResultTopic(
            client_id=self.client.id,
            expected_operation_state_id=self.expected_operation_state_id,
            expected_auto_transfer_type=0,
        )
        with allure.step(f'Чтение сообщения из топика {self.topic.name}'):
            self.message_from_topic = self.kafka.wait_message(
                topic_name=self.topic.name,
                search_criteria={'clientId': self.client.id},
            )
            allure_attach(self.message_from_topic, self.payment_result_attach_name)


class TestPositive(_CallbackPaymentStatusBase):
    pytestmark = allure.story('Positive. Успешный callback публикует Portfolio.Payments.Result')

    terminal_id = 'PORTFOLIO_AUTOTEST_ORDINARY_DEBIT_001'
    state = 'APPROVED'
    reason_code = '1'
    message = 'Successful financial transaction'
    expected_operation_state_id = 20

    @allure.title('Успешный callback публикует Portfolio.Payments.Result')
    def test_callback_success_publishes_payment_result(self):
        with allure.step('Проверка полей сообщения'):
            self.topic.check_message(self.message_from_topic)


class TestNegative(_CallbackPaymentStatusBase):
    pytestmark = allure.story('Negative. Callback с отказом партнёра публикует Portfolio.Payments.Result')

    terminal_id = 'PORTFOLIO_AUTOTEST_ORDINARY_DEBIT_001'
    state = 'REJECTED'
    reason_code = '51'
    message = 'Insufficient funds'
    expected_operation_state_id = 25
    payment_result_attach_name = 'payment_result_message_error'

    @allure.title('Callback с отказом партнёра публикует Portfolio.Payments.Result с ошибкой')
    def test_callback_error_publishes_payment_result(self):
        with allure.step('Проверка полей сообщения (ошибка)'):
            self.topic.check_message(self.message_from_topic)
