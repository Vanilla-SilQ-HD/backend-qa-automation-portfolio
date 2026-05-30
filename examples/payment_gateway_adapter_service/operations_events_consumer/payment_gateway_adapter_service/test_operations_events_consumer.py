from datetime import UTC, datetime

import pytest
from hamcrest import assert_that, equal_to, is_in, not_none

import config
from helpers import data_generator, get_test_uuid
from helpers.allure_client import allure, allure_attach
from kafka_topics.internal.portfolio_operations_events import PortfolioOperationsEventsTopic
from kafka_topics.internal.portfolio_payments_result_flow import PortfolioPaymentsResultFlexibleTopic
from payment_gateway_adapter_service import service_name


pytestmark = [
    allure.epic(service_name),
    allure.feature('Portfolio.Operations.Events -> PaymentGatewayAdapterService'),
    allure.label('microservice', service_name),
    allure.label('owner', 'portfolio'),
    allure.description(
        'PaymentGatewayAdapterService обрабатывает событие создания платежной операции, '
        'регистрирует ее у внешнего провайдера и публикует Portfolio.Payments.Result.'
    ),
    allure.link('https://example.com/spec-redacted', name='Specification'),
    allure.severity('critical'),
    allure.tag(service_name),
    allure.tag('Kafka'),
]


class TestPositive:
    pytestmark = allure.story('Positive. Операция зарегистрирована у платежного провайдера')

    @allure.title('Подготовка данных для тестов')
    @pytest.fixture(scope='class', autouse=True)
    def prepare(self, request, kafka):
        self = request.cls
        self.kafka = kafka
        self._before(self)
        self._send_message(self)
        self._after(self)

    def _before(self):
        with allure.step('Генерируем клиента и входящее Portfolio.Operations.Events'):
            self.client = data_generator.Client()
            self.operation_id = int(datetime.now(UTC).timestamp() * 1000) % 1_000_000_000_000 + 10_001
            self.amount = 150_00
            self.event_time = datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')

            topic = PortfolioOperationsEventsTopic(
                operation_id=self.operation_id,
                operation_state_id=10,
                operation_state_name='Created',
                operation_type_id=25,
                operation_type_name='P2PCredit',
                client_id=str(self.client.id),
                account_id=str(get_test_uuid()),
                amount=self.amount,
                currency=643,
                fee=0,
                description='Автотест PaymentGatewayAdapter: пополнение кошелька',
                msisdn=self.client.msisdn,
                event_time=self.event_time,
            )
            self.message, self.key, self.headers = topic.set_message()
            self.input_topic = topic.name
            allure_attach(self.message, 'operations_event_message')

    def _send_message(self):
        with allure.step(f'Отправляем сообщение в {self.input_topic}'):
            self.kafka.send_message(
                topic=self.input_topic,
                message=self.message,
                key=self.key,
                headers=self.headers,
            )

    def _after(self):
        with allure.step(f'Ожидаем сообщение в {config.portfolio_payments_result_topic}'):
            self.payment_result_message = self.kafka.wait_message(
                topic_name=config.portfolio_payments_result_topic,
                search_criteria={'operationId': self.operation_id, 'clientId': str(self.client.id)},
                timeout=90,
                time_shift=600,
            )
            allure_attach(self.payment_result_message, 'payment_result_message')

        with allure.step('Разбираем Portfolio.Payments.Result'):
            parser = PortfolioPaymentsResultFlexibleTopic()
            self.payment_result = parser.parse(self.payment_result_message)
            allure_attach(self.payment_result.model_dump(mode='json'), 'payment_result_parsed')

    @allure.title('PaymentGatewayAdapter публикует Portfolio.Payments.Result по нашей операции')
    def test_payment_result_event_published(self):
        assert_that(self.payment_result_message, not_none(), 'Сообщение Portfolio.Payments.Result не получено')
        assert_that(
            self.payment_result.operationId,
            equal_to(self.operation_id),
            'В сообщении должен быть наш operationId',
        )
        assert_that(str(self.payment_result.clientId), equal_to(str(self.client.id)), 'clientId должен совпасть')
        assert_that(self.payment_result.operationTypeId, equal_to(25), 'Ожидается тип P2PCredit')
        assert_that(self.payment_result.operationStateId, is_in((15, 25)), 'Ожидается Pending или Error')

    @allure.title('Portfolio.Payments.Result содержит данные успешной регистрации или ошибки')
    def test_payment_result_event_payload(self):
        assert_that(self.payment_result.operationStateId, not_none(), 'operationStateId обязателен')
        if self.payment_result.operationStateId == 15:
            has_provider_reference = (
                self.payment_result.partnerOrderId is not None
                or self.payment_result.sectorId is not None
            )
            assert_that(has_provider_reference, equal_to(True), 'При успехе ожидается provider reference')
        if self.payment_result.operationStateId == 25:
            has_error_context = (
                self.payment_result.reasonCodeId is not None
                or self.payment_result.message is not None
            )
            assert_that(has_error_context, equal_to(True), 'При ошибке ожидается reasonCodeId или message')
