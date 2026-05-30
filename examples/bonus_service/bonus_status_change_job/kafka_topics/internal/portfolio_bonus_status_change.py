from datetime import UTC, datetime

from hamcrest import equal_to, not_none
from kafka.consumer.fetcher import ConsumerRecord
from pydantic import ConfigDict

import config
from helpers import check_assert_that
from helpers.pydantic_base_model import BasePydanticModel
from kafka_topics.internal import BaseTopic


class StatusChangeMessage(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    clientId: str
    changeDate: str


class PortfolioBonusStatusChangeTopic(BaseTopic):
    event_type = 'bonusStatusChange'

    def __init__(self, client_id: str):
        self.name = config.bonus_status_change_topic
        self.client_id = client_id

    def set_message(self):
        headers = self._set_headers()
        headers.append(('eventType', self.event_type.encode()))
        value = {
            'clientId': self.client_id,
            'changeDate': datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S+00:00'),
        }
        StatusChangeMessage.model_validate(value)
        key = self.client_id
        return value, key, headers

    def check_message(self, message: ConsumerRecord):
        check_assert_that(message, not_none(), 'Сообщение отсутствует')
        check_assert_that(message.topic, equal_to(self.name), 'Ошибка в названии топика')
        check_assert_that(message.key, not_none(), 'Ошибка в ключе')
        check_assert_that(message.key, equal_to(self.client_id), 'Ошибка в ключе (clientId)')

        if message.headers:
            headers = self._get_headers_from_message(message)
            if 'eventType' in headers:
                check_assert_that(
                    headers.get('eventType'),
                    equal_to(self.event_type),
                    'Ошибка в eventType',
                )

        val = message.value
        parsed = StatusChangeMessage.model_validate(val)
        check_assert_that(parsed.clientId, equal_to(self.client_id), 'Ошибка в clientId')
        check_assert_that(parsed.changeDate, not_none(), 'Ошибка в changeDate')
        check_assert_that(
            bool(parsed.changeDate),
            equal_to(True),
            'Ошибка в changeDate: пустая строка',
        )
