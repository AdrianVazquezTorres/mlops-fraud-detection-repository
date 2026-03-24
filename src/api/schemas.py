from pydantic import BaseModel, Field


class BankTransaction(BaseModel):
    # Agregamos Field() a algunos campos para agregar reglas extra y/o contexto de las variables
    # Los 3 puntos significan que el campo DEBE SER OBLIGATORIO, si no se envía mínimo esta información
    # el código del modelo ni siquiera se ejecuta.
    step: int = Field(..., description="Hour of transaction (1 step = 1 hour)")
    type: str = Field(..., description="Type of transaction")
    amount: float = Field(..., gt=0, description="Amount of the transaction (must be greater than 0)")
    oldbalanceOrg: float
    newbalanceOrig: float
    oldbalanceDest: float
    newbalanceDest: float
