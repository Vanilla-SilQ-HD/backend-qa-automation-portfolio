"""Компактные response models для GET /v1/history/byClient с только используемыми полями."""

from pydantic import ConfigDict, Field

from helpers.pydantic_base_model import BasePydanticModel


class OperationState(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    id: int
    name: str


class OperationType(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    id: int
    name: str


class HistoryOperation(BasePydanticModel):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)

    transactionId: int
    accountId: str
    accountType: int
    amount: int
    currency: int
    operationDate: str
    operationState: OperationState = Field(alias='state')
    operationType: OperationType = Field(alias='type')


class HistoryByClientGetResponse(BasePydanticModel):
    model_config = ConfigDict(extra='ignore')

    operations: list[HistoryOperation]
