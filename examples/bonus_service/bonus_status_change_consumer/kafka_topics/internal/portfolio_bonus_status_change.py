from datetime import UTC, datetime

from pydantic import ConfigDict

import config
from helpers.pydantic_base_model import BasePydanticModel
from kafka_topics.internal import BaseTopic


class StatusChangeMessage(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    clientId: str
    changeDate: str


class PortfolioBonusStatusChangeTopic(BaseTopic):
    event_type = 'bonusStatusChange'

    def __init__(self, client_id):
        self.name = config.bonus_status_change_topic
        self.client_id = str(client_id)

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
