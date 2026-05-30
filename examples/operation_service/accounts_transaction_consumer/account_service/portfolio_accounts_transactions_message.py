from pydantic import BaseModel, ConfigDict, Field


class PortfolioAccountsTransactionsMessage(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    transactionId: int
    accountId: str
    accountTypeId: int
    clientId: str
    operationId: int | None
    event: int
    eventTime: str
    currency: int
    amount: int
    type_: int = Field(alias="type")
    status: int
    description: str
    createdAt: str
