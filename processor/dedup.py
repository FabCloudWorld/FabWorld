import time

SUPPRESS_MS = 60 * 1000  # 60초 억제


class DedupFilter:
    def __init__(self):
        # (chamberId, sensorType) → last alert timestamp ms
        self._last: dict[tuple[str, str], int] = {}

    def should_send(self, event: dict) -> bool:
        key = (event["chamberId"], event["sensorType"])
        now = time.time_ns() // 1_000_000
        if now - self._last.get(key, 0) >= SUPPRESS_MS:
            self._last[key] = now
            return True
        return False