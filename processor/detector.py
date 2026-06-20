import statistics
import time
from collections import deque

THRESHOLDS = {
    "temp":         380.0,
    "pressure":     3.0,
    "rf_power":     1800.0,
    "gas_flow":     250.0,
    "bias_voltage": 450.0,
}

ANOMALY_TYPES = {
    "temp":         "TEMP_SPIKE",
    "pressure":     "PRESS_DROP",
    "rf_power":     "RF_UNSTABLE",
    "gas_flow":     "GAS_FLOW_ERR",
    "bias_voltage": "BIAS_ERR",
}

WINDOW_SIZE = 60
SIGMA_MULT  = 3.0


class _Window:
    def __init__(self):
        self._buf: deque[float] = deque(maxlen=WINDOW_SIZE)

    def push(self, v: float) -> None:
        self._buf.append(v)

    def is_sigma_anomaly(self, v: float) -> bool:
        if len(self._buf) < 10:
            return False
        mu = statistics.mean(self._buf)
        sigma = statistics.stdev(self._buf)
        return sigma > 0 and abs(v - mu) > SIGMA_MULT * sigma


class AnomalyDetector:
    def __init__(self):
        self._windows: dict[tuple[str, str], _Window] = {}

    def _window(self, chamber: str, sensor: str) -> _Window:
        key = (chamber, sensor)
        if key not in self._windows:
            self._windows[key] = _Window()
        return self._windows[key]

    def check(self, event: dict) -> dict | None:
        """이상 탐지 시 anomaly 필드를 추가한 dict 반환, 정상이면 None."""
        chamber = event["chamberId"]
        sensor  = event["sensorType"]
        value   = event["value"]
        win     = self._window(chamber, sensor)
        threshold = THRESHOLDS.get(sensor)

        anomaly_type = reason = None

        if threshold and value > threshold:
            anomaly_type = ANOMALY_TYPES[sensor]
            reason       = f"val={value} > threshold={threshold}"
        elif win.is_sigma_anomaly(value):
            anomaly_type = ANOMALY_TYPES[sensor]
            reason       = f"val={value} exceeds 3σ from rolling mean"

        win.push(value)

        if anomaly_type is None:
            return None

        return {
            **event,
            "isAnomaly":   True,
            "anomalyType": anomaly_type,
            "severity":    _severity(value, threshold),
            "reason":      reason,
            "detectedAt":  time.time_ns() // 1_000_000,
        }


def _severity(value: float, threshold: float | None) -> str:
    if threshold is None:
        return "LOW"
    ratio = value / threshold
    if ratio >= 1.1:
        return "HIGH"
    if ratio >= 1.0:
        return "MEDIUM"
    return "LOW"