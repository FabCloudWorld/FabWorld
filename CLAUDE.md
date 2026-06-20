# CLAUDE.md — FAB WORLD 프로젝트

## 프로젝트 개요
반도체 Fab 이벤트 수집 및 이상 탐지 시스템.
CloudClub 해커톤 프로젝트. 설계 문서: FAB-WORLD-DESIGN.md 참고.

## 디렉토리 구조
```
fab-world/
├── CLAUDE.md
├── FAB-WORLD-DESIGN.md
├── docker-compose.yml
├── .env                    ← 실제 환경변수 (gitignore됨)
├── .env.example            ← 환경변수 템플릿
├── fab-world.html          ← 게임 시뮬레이터 (완료)
├── producer/               ← FastAPI Producer
│   ├── main.py             ← FastAPI app, POST /events
│   ├── kafka_producer.py   ← aiokafka produce
│   ├── schema.py           ← Pydantic SensorEvent
│   └── requirements.txt
├── burst/                  ← Burst Generator
│   ├── burst_generator.py  ← --rate N events/sec
│   └── requirements.txt
├── processor/              ← Stream Processor
│   ├── main.py             ← consumer 메인루프
│   ├── detector.py         ← 임계치 + 3σ 이상 탐지
│   ├── dedup.py            ← 60초 중복 알림 억제
│   ├── alerter.py          ← Slack Webhook 발송
│   └── requirements.txt
├── consumer/               ← ClickHouse INSERT Consumer
│   ├── ch_consumer.py      ← 배치 INSERT (2000건 or 2초)
│   └── requirements.txt
├── clickhouse/             ← DB 스키마
│   └── init.sql            ← sensor_raw / anomaly_events / MV
└── grafana/
    ├── datasources/
    │   └── clickhouse.yml  ← ClickHouse 데이터소스 자동 프로비저닝
    └── dashboards/
        ├── provider.yml    ← 대시보드 폴더 프로바이더
        └── dashboard.json  ← FAB WORLD Monitor 대시보드
```

## 기술 스택
- Python 3.11+
- FastAPI + uvicorn
- aiokafka
- ClickHouse (MergeTree)
- Grafana
- Docker Compose

## Kafka 토픽
```
sensor-raw      파티션 20개, retention 48h, key=chamberId
anomaly-events  파티션  4개, retention  7d, key=equipmentId
alert-out       파티션  2개, retention  3d
```

## 메시지 스키마

### sensor-raw 토픽
```python
{
    "equipmentId": str,   # "EQ-001"
    "chamberId":   str,   # "CH-042"
    "sensorType":  str,   # "temp" | "pressure" | "rf_power" | "gas_flow" | "bias_voltage"
    "value":       float,
    "timestamp":   int,   # Unix ms
    "waferId":     str,   # "W-5821"
    "lotId":       str,   # "L-2847"
    "recipeId":    str,   # "RCP-CVD-001"
    "isAnomaly":   bool,  # Producer 단계에선 항상 False
}
```

### anomaly-events 토픽
sensor-raw 필드 전체 +
```python
{
    "isAnomaly":   True,
    "anomalyType": str,   # "TEMP_SPIKE" | "PRESS_DROP" | "RF_UNSTABLE" | "GAS_FLOW_ERR" | "BIAS_ERR"
    "severity":    str,   # "LOW" | "MEDIUM" | "HIGH"
    "reason":      str,   # "val=392.0 > threshold=380"
    "detectedAt":  int,   # Unix ms
}
```

## 센서 임계치
```python
THRESHOLDS = {
    "temp":         380,
    "pressure":     3.0,
    "rf_power":     1800,
    "gas_flow":     250,
    "bias_voltage": 450,
}
```

## 실행 순서
```bash
# 1. 인프라
docker-compose up -d

# 2. Producer
cd producer && uvicorn main:app --reload --port 8000

# 3. Stream Processor
cd processor && python main.py

# 4. Burst Generator (부하 테스트)
cd burst && python burst_generator.py --rate 5000

# 5. 게임 시뮬레이터
# fab-world.html 브라우저에서 열기
```

## 컨벤션
- Kafka 클라이언트: aiokafka만 사용 (kafka-python 혼용 금지)
- timestamp: 항상 Unix ms (int) — time.time_ns() // 1_000_000
- JSON: json.dumps(event).encode("utf-8")
- 환경변수: .env 파일 (KAFKA_BOOTSTRAP, CLICKHOUSE_HOST 등)
- 에러 처리: 해커톤 단순화 → 로그 출력 후 skip (DLQ 생략)

## 친구 연동 포인트
- consume 대상: sensor-raw, anomaly-events 토픽
- Kafka: localhost:9092
- ClickHouse HTTP: localhost:8123
- 스키마 상세: FAB-WORLD-DESIGN.md 3절

## 구현 상태
- [x] fab-world.html (게임 시뮬레이터)
- [x] docker-compose.yml (Zookeeper, Kafka, ClickHouse, Grafana, kafka-ui)
- [x] producer/ (FastAPI + aiokafka)
- [x] burst/ (burst_generator.py)
- [x] processor/ (detector, dedup, alerter, main)
- [x] consumer/ (ch_consumer.py — 친구 파트 미리 구현, feature/edge 브랜치)
- [x] clickhouse/init.sql (친구 파트 미리 구현, feature/edge 브랜치)
- [x] grafana/dashboard.json (친구 파트 미리 구현, feature/edge 브랜치)

## 브랜치 전략
- `feature/edge` — 전체 구현 완료 (데모/검증용)
- `main` — 민섭 파트만 머지 예정 (친구 인계 기준점)

## Slack Webhook
- SLACK_WEBHOOK_URL은 .env에 설정됨 (gitignore됨, .env.example 참고)
- 60초 dedup 적용: 같은 (chamberId, sensorType) 조합은 60초 내 재알림 억제
