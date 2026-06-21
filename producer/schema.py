from typing import Literal
from pydantic import BaseModel


class SensorEvent(BaseModel):
    equipmentId: str
    chamberId:   str
    sensorType:  Literal["temp", "pressure", "rf_power", "gas_flow", "bias_voltage"]
    value:       float
    timestamp:   int = 0       # Unix ms; 0이면 producer가 채움
    waferId:     str = ""
    lotId:       str = ""
    recipeId:    str = ""
    isAnomaly:   bool = False