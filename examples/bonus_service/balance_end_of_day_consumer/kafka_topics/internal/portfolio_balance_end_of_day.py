from pydantic import ConfigDict

import config
from helpers.pydantic_base_model import BasePydanticModel
from kafka_topics.internal import BaseTopic


class BalanceEndOfDayTrace(BasePydanticModel):
    model_config = ConfigDict(extra="ignore")

    notifyId: str
    requestId: str
    requestTime: str


class BalanceEndOfDayBody(BasePydanticModel):
    model_config = ConfigDict(extra="ignore")

    accountId: str
    accountType: int
    clientId: str
    eventType: str
    eventTime: str
    balance: int
    balanceDay: str


class BalanceEndOfDayMessage(BasePydanticModel):
    model_config = ConfigDict(extra="ignore")

    trace: BalanceEndOfDayTrace
    accountBalance: BalanceEndOfDayBody


class PortfolioBalanceEndOfDayTopic(BaseTopic):
    event_type = "balanceEndOfDay"

    def __init__(self, context: dict):
        self.name = config.balance_end_of_day_topic
        self.context = context

    def set_message(self):
        value = {
            "trace": {
                "notifyId": "balanceEndOfDay",
                "requestId": self.context["request_id"],
                "requestTime": self.context["balance_day"],
            },
            "accountBalance": {
                "accountId": self.context["account_id"],
                "accountType": self.context["account_type"],
                "clientId": self.context["client_id"],
                "eventType": "balanceEndOfDay",
                "eventTime": self.context["balance_day"],
                "balance": self.context["balance"],
                "balanceDay": self.context["balance_day"],
            },
        }
        BalanceEndOfDayMessage.model_validate(value)
        key = self.context["client_id"]
        headers = self._set_headers()
        headers.append(("eventType", self.event_type.encode()))
        return value, key, headers
