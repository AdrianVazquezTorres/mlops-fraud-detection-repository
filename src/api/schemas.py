from pydantic import BaseModel, Field


class BankTransaction(BaseModel):
    step: int = Field(..., description="Hour of transaction (1 step = 1 hour)")
    type: str = Field(..., description="Type of transaction")
    amount: float = Field(..., gt=0, description="Amount of the transaction (must be greater than 0)")
    oldbalanceOrg: float
    newbalanceOrig: float
    oldbalanceDest: float
    newbalanceDest: float
