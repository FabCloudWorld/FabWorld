"""더미 데이터 시드 스크립트.

Kafka / burst 없이 ClickHouse에 직접 과거 데이터를 INSERT 해서
Grafana 대시보드 패널을 바로 확인할 수 있게 한다.

- sensor_raw 에 INSERT → MV(mv_sensor_1min)가 sensor_1min_agg 자동 집계
- anomaly_events 에 일부 챔버 이상 이벤트 INSERT → 이상 패널/이상 챔버 패널용

실행:
    py -3.12 seed/seed_data.py                  # 기본: 최근 60분, 30초 간격
    py -3.12 seed/seed_data.py --minutes 120 --interval 15
    py -3.12 seed/seed_data.py --anomaly-chambers CVD-A1,ETCH-C1
"""
import argparse
import json
import os
import random
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CH_DB   = os.getenv("CLICKHOUSE_DB",   "fabworld")
CH_USER = os.getenv("CLICKHOUSE_USER", "grafana")
CH_PASS = os.getenv("CLICKHOUSE_PASS", "fab123")
CH_URL  = f"http://{CH_HOST}:{CH_PORT}/"

CHAMBERS = [
    "CVD-A1", "CVD-A2", "PVD-B1", "PVD-B2", "ETCH-C1",
    "ETCH-C2", "INSP-D1", "INSP-D2", "CMP-E1", "CMP-E2",
]
SENSOR_TYPES = ["temp", "pressure", "rf_power", "gas_flow", "bias_voltage"]

# burst_generator.py 와 동일한 정상 범위
NORMAL_RANGES = {
    "temp":         (280.0, 350.0),
    "pressure":     (1.8,   2.5),
    "rf_power":     (800.0, 1500.0),
    "gas_flow":     (50.0,  200.0),
    "bias_voltage": (100.0, 400.0),
}
# CLAUDE.md 임계치
THRESHOLDS = {
    "temp": 380.0, "pressure": 3.0, "rf_power": 1800.0,
    "gas_flow": 250.0, "bias_voltage": 450.0,
}
ANOMALY_TYPE = {
    "temp": "TEMP_SPIKE", "pressure": "PRESS_DROP", "rf_power": "RF_UNSTABLE",
    "gas_flow": "GAS_FLOW_ERR", "bias_voltage": "BIAS_ERR",
}

_RAW_SQL = (
    f"INSERT INTO {CH_DB}.sensor_raw "
    "(equipmentId,chamberId,sensorType,value,timestamp,waferId,lotId,recipeId,isAnomaly) "
    "FORMAT JSONEachRow"
)
_ANOMALY_SQL = (
    f"INSERT INTO {CH_DB}.anomaly_events "
    "(equipmentId,chamberId,sensorType,value,timestamp,waferId,lotId,recipeId,"
    "isAnomaly,anomalyType,severity,reason,detectedAt) "
    "FORMAT JSONEachRow"
)


def _severity(value: float, threshold: float) -> str:
    ratio = value / threshold
    if ratio >= 1.1:
        return "HIGH"
    if ratio >= 1.0:
        return "MEDIUM"
    return "LOW"


def _base_event(chamber: str, sensor: str, value: float, ts_ms: int) -> dict:
    return {
        "equipmentId": f"EQ-{chamber}",
        "chamberId":   chamber,
        "sensorType":  sensor,
        "value":       round(value, 2),
        "timestamp":   ts_ms,
        "waferId":     f"W-{random.randint(1000, 9999)}",
        "lotId":       f"L-{random.randint(1000, 9999)}",
        "recipeId":    "RCP-SEED-001",
    }


def build_rows(minutes: int, interval: int, anomaly_chambers: set[str]):
    raw_rows: list[dict] = []
    anomaly_rows: list[dict] = []

    now_ms = time.time_ns() // 1_000_000
    start_ms = now_ms - minutes * 60_000

    ts = start_ms
    while ts <= now_ms:
        for chamber in CHAMBERS:
            for sensor in SENSOR_TYPES:
                lo, hi = NORMAL_RANGES[sensor]
                value = random.uniform(lo, hi)

                # 이상 챔버는 ~8% 확률로 임계치 초과 값 주입
                inject = chamber in anomaly_chambers and random.random() < 0.08
                if inject:
                    thr = THRESHOLDS[sensor]
                    value = thr * random.uniform(1.02, 1.25)

                ev = _base_event(chamber, sensor, value, ts)
                ev["isAnomaly"] = inject
                raw_rows.append(ev)

                if inject:
                    thr = THRESHOLDS[sensor]
                    a = dict(ev)
                    a.update({
                        "anomalyType": ANOMALY_TYPE[sensor],
                        "severity":    _severity(value, thr),
                        "reason":      f"val={round(value, 1)} > threshold={thr}",
                        "detectedAt":  ts,
                    })
                    anomaly_rows.append(a)
        ts += interval * 1000

    return raw_rows, anomaly_rows


def insert(client: httpx.Client, sql: str, rows: list[dict]) -> None:
    if not rows:
        return
    body = "\n".join(json.dumps(r) for r in rows).encode()
    resp = client.post(
        CH_URL,
        params={"query": sql, "user": CH_USER, "password": CH_PASS},
        content=body,
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"CH error {resp.status_code}: {resp.text[:300]}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=int, default=60, help="과거 몇 분 데이터 생성")
    parser.add_argument("--interval", type=int, default=30, help="샘플 간격(초)")
    parser.add_argument(
        "--anomaly-chambers",
        default="CVD-A1,ETCH-C1,PVD-B2",
        help="이상 주입 챔버 (콤마 구분)",
    )
    args = parser.parse_args()

    anomaly_chambers = {c.strip() for c in args.anomaly_chambers.split(",") if c.strip()}
    raw_rows, anomaly_rows = build_rows(args.minutes, args.interval, anomaly_chambers)

    with httpx.Client() as client:
        insert(client, _RAW_SQL, raw_rows)
        insert(client, _ANOMALY_SQL, anomaly_rows)

    print(f"[seed] sensor_raw     : {len(raw_rows):,} rows")
    print(f"[seed] anomaly_events : {len(anomaly_rows):,} rows  (chambers={sorted(anomaly_chambers)})")
    print("[seed] done. Grafana 대시보드에서 확인하세요.")


if __name__ == "__main__":
    main()
