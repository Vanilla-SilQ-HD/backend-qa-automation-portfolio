"""Portfolio.Payments.Result: разбор для PaymentGatewayAdapter после Register (поля могут отличаться от сценария автосписания с накопа)."""

from pydantic import BaseModel, ConfigDict

import config
from kafka_topics.internal import BaseTopic


class PortfolioPaymentsResultFlexible(BaseModel):
    model_config = ConfigDict(extra='ignore')

    operationStateId: int
    operationId: int | None = None
    operationTypeId: int | None = None
    clientId: str | None = None
    eventTime: str | None = None
    partnerOrderId: str | int | None = None
    partnerOrderDate: str | None = None
    reasonCodeId: int | None = None
    message: str | None = None
    sectorId: int | None = None
    amount: int | None = None
    currency: int | None = None


class PortfolioPaymentsResultFlexibleTopic(BaseTopic):
    def __init__(self):
        self.name = config.portfolio_payments_result_topic

    @staticmethod
    def parse(message) -> PortfolioPaymentsResultFlexible:
        return PortfolioPaymentsResultFlexible.model_validate(message.value)
