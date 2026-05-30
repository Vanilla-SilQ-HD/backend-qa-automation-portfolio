from typing import List, Optional

from pydantic import ConfigDict

from helpers.pydantic_base_model import BasePydanticModel


class BonusAccrualRule(BasePydanticModel):
    model_config = ConfigDict(extra="ignore")

    ruleId: int
    ruleType: str
    accountType: str
    operationCategory: Optional[str] = None
    bonusPercentage: float
    minAmountForBonus: Optional[int] = None
    maxAmountForBonus: Optional[int] = None
    burningPeriodMonths: Optional[int] = None
    burningPeriodDay: Optional[int] = None
    activationMonth: Optional[int] = None
    activationDay: Optional[int] = None
    currency: Optional[int] = None
    bonusStatus: Optional[int] = None
    isActive: Optional[bool] = None
    validFrom: str
    validTo: str


class BonusAccrualsResponse(BasePydanticModel):
    model_config = ConfigDict(extra="ignore")

    rules: List[BonusAccrualRule]


class BonusAccrualsErrorResponse(BasePydanticModel):
    model_config = ConfigDict(extra="ignore")

    message: str
    warnings: list[str]
