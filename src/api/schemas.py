from pydantic import BaseModel, Field


class BankTransaction(BaseModel):
    step: str = Field(..., description="The step of the transaction")
    type: str = Field(..., description="Type of transaction")
    amount: float = Field(..., gt=0, description="Amount of the transaction (must be greater than 0)")
    oldbalanceOrg: float
    newbalanceOrig: float
    oldbalanceDest: float
    newbalanceDest: float
