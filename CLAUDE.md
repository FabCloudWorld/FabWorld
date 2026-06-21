# CLAUDE.md — FAB WORLD 프로젝트

## 프로젝트 개요
반도체 Fab 이벤트 수집 및 이상 탐지 시스템.
CloudClub 해커톤 프로젝트. 설계 문서: FAB-WORLD-DESIGN.md 참고.

---

## 아키텍처 전체 흐름

```
[fab-world.html]  ─── HTTP POST /events ───►  [FastAPI Producer]
                                                     │
                                            Kafka: sensor-raw
                                                     │
                                        ┌────────────┴────────────┐
                                        │                         │
                              [Stream Processor]         [ClickHouse Consumer]
                              detector.py (임계치+3σ)      ch_consumer.py
                              dedup.py (60초 중복억제)      (배치 INSERT)
                              alerter.py (Slack)                  │
                                        │                         ▼
                              Kafka: anomaly-events         [ClickHouse]
                                        │                    sensor_raw
                                        └────────────────►   anomaly_events
                                                             sensor_1min_agg (MV)
                                                                  │
                                                                  ▼
                                                            [Grafana]
                                                         FAB WORLD Monitor

[Burst Generator] ─── Kafka: sensor-raw (5000건/sec) ───► 대규모 부하 테스트
```

**데이터 흐름 요약:**
1. HTML 시뮬레이터에서 이상 주입 버튼 → FastAPI POST /events
2. FastAPI → Kafka `sensor-raw` 토픽으로 produce
3. Processor: `sensor-raw` consume → 이상 탐지 → `anomaly-events` produce + Slack 알림
4. Consumer: `sensor-raw` + `anomaly-events` 동시 consume → ClickHouse INSERT
5. ClickHouse Materialized View → `sensor_1min_agg` 자동 집계
6. Grafana → ClickHouse HTTP 쿼리로 실시간 시각화

---

## 디렉토리 구조

```
fab-world/
├── CLAUDE.md
├── FAB-WORLD-DESIGN.md
├── docker-compose.yml
├── .env                    ← 실제 환경변수 (gitignore됨, 팀원에게 직접 전달)
├── fab-world.html          ← 게임 시뮬레이터 (완료)
├── producer/               ← FastAPI Producer
│   ├── main.py             ← FastAPI app, POST /events, CORS 허용
│   ├── kafka_producer.py   ← aiokafka AIOKafkaProducer
│   ├── schema.py           ← Pydantic SensorEvent 모델
│   └── requirements.txt
├── burst/                  ← Burst Generator
│   ├── burst_generator.py  ← --rate N events/sec
│   └── requirements.txt
├── processor/              ← Stream Processor
│   ├── main.py             ← consumer 메인루프, load_dotenv 최상단
│   ├── detector.py         ← 임계치 + 3σ 이상 탐지
│   ├── dedup.py            ← 60초 중복 알림 억제
│   ├── alerter.py          ← Slack Webhook (지연 로딩 패턴)
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

---

## 기술 스택

| 구성요소 | 기술 | 비고 |
|---|---|---|
| 언어 | Python 3.12 | 3.14는 aiokafka wheel 없음 → 빌드 실패 |
| Web API | FastAPI + uvicorn | POST /events |
| 메시지 큐 | Apache Kafka (Confluent 7.6.1) | aiokafka 클라이언트 |
| 스토리지 | ClickHouse 24.3 | MergeTree, DateTime64(3) |
| 시각화 | Grafana 10.4.2 | grafana-clickhouse-datasource 플러그인 |
| 알림 | Slack Incoming Webhook | httpx 비동기 POST |
| 컨테이너 | Docker Compose | 6개 서비스 |

---

## .env 환경변수

```
KAFKA_BOOTSTRAP=localhost:9092
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DB=fabworld
CLICKHOUSE_USER=grafana
CLICKHOUSE_PASS=fab123
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

**주의:** `.env` 파일은 gitignore됨. 팀원에게 직접 파일 전달할 것.

---

## Kafka 토픽

```
sensor-raw      파티션 20개, retention 48h
anomaly-events  파티션  4개, retention  7d
alert-out       파티션  2개, retention  3d
```

**핵심:** `kafka-init` 컨테이너가 docker-compose up 시 자동으로 토픽 생성.
`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` 이므로 수동 생성 필수.

### Kafka 듀얼 리스너 구조

```
PLAINTEXT://kafka:29092      ← 컨테이너끼리 통신 (processor, consumer가 docker 내에서 실행 시)
PLAINTEXT_HOST://localhost:9092  ← 호스트에서 접속 (Python 프로세스: aiokafka bootstrap)
```

호스트에서 Python으로 실행하는 경우 반드시 `localhost:9092` 사용.

---

## 메시지 스키마

### sensor-raw 토픽
```python
{
    "equipmentId": str,   # "EQ-CVD-A1"
    "chamberId":   str,   # "CVD-A1"
    "sensorType":  str,   # "temp" | "pressure" | "rf_power" | "gas_flow" | "bias_voltage"
    "value":       float,
    "timestamp":   int,   # Unix ms
    "waferId":     str,
    "lotId":       str,
    "recipeId":    str,
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

---

## 센서 임계치 및 이상 탐지 로직

```python
THRESHOLDS = {
    "temp":         380.0,
    "pressure":     3.0,
    "rf_power":     1800.0,
    "gas_flow":     250.0,
    "bias_voltage": 450.0,
}
```

**탐지 방법 (detector.py):**
1. **임계치 초과**: `value > THRESHOLDS[sensor]` → 즉시 이상 판정
2. **3σ 이상**: 최근 60개 샘플 슬라이딩 윈도우, 10개 이상 쌓이면 `|value - mean| > 3 * stdev` 체크

**Severity 판정:**
- `value / threshold >= 1.1` → HIGH
- `value / threshold >= 1.0` → MEDIUM
- 그 외 (3σ 탐지) → LOW

**중복 억제 (dedup.py):**
- 같은 `(chamberId, sensorType)` 조합은 60초 내 재알림 없음
- Slack으로만 억제 적용; anomaly-events 토픽 produce는 항상 수행

---

## ClickHouse 스키마

### sensor_raw
```sql
CREATE TABLE fabworld.sensor_raw (
    equipmentId String,
    chamberId   String,
    sensorType  LowCardinality(String),
    value       Float64,
    timestamp   DateTime64(3),           -- Unix ms 자동 변환
    ...
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (chamberId, sensorType, timestamp)
TTL toDateTime(timestamp) + INTERVAL 7 DAY;
```

### anomaly_events
```sql
CREATE TABLE fabworld.anomaly_events (
    ...
    detectedAt  DateTime64(3),
    ...
) ENGINE = MergeTree()
ORDER BY (chamberId, sensorType, detectedAt)
TTL toDateTime(detectedAt) + INTERVAL 365 DAY;
```

### Materialized View (1분 집계)
```sql
CREATE MATERIALIZED VIEW fabworld.mv_sensor_1min
TO fabworld.sensor_1min_agg AS
SELECT chamberId, sensorType,
       toStartOfMinute(timestamp) AS minute,
       avg(value), max(value), min(value), count()
FROM fabworld.sensor_raw
GROUP BY chamberId, sensorType, minute;
```
→ sensor_raw에 INSERT 시 자동으로 sensor_1min_agg에도 집계 데이터 기록.
→ Grafana Throughput 패널이 이 집계 테이블을 쿼리.

### ClickHouse 사용자
- `grafana` 유저 (비번: `fab123`): ALL ON fabworld.* + SELECT ON system.*
- `default` 유저는 users.xml readonly → SQL로 ALTER 불가, 사용하지 않음

---

## 각 컴포넌트 상세

### producer/main.py
- FastAPI + CORSMiddleware (`allow_origins=["*"]`) — HTML에서 직접 fetch 가능
- `POST /events`: SensorEvent 검증 → timestamp 없으면 현재 시각 채움 → kafka_producer.produce()
- `load_dotenv`는 파일 최상단에서 절대경로로 호출

### producer/kafka_producer.py
```python
_producer = AIOKafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    linger_ms=5,
    max_batch_size=65536,      # aiokafka 0.11에서 batch_size → max_batch_size로 변경됨
    compression_type="gzip",   # lz4는 C 확장 없이 동작 안 함 → gzip 사용
    acks="all",
)
```

### processor/alerter.py
```python
# 핵심 패턴: 모듈 수준에서 os.getenv() 호출하면 load_dotenv() 이전에 읽어서 항상 빈값
def _webhook_url() -> str:
    return os.getenv("SLACK_WEBHOOK_URL", "")   # 호출 시점에 읽기

async def send_slack(event: dict) -> None:
    webhook_url = _webhook_url()   # 함수 실행 시 env 조회
```

### processor/main.py
```python
# load_dotenv는 반드시 로컬 모듈 import 전에 호출!
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from alerter import send_slack   # ← 이 import 시점엔 이미 env 로드 완료
```

### consumer/ch_consumer.py
- `sensor-raw`와 `anomaly-events` 동시 consume
- 배치 전략: 2000건 쌓이거나 2초 경과 시 flush
- ClickHouse HTTP INSERT: `INSERT INTO fabworld.sensor_raw FORMAT JSONEachRow`
- grafana 유저로 접속 (CLICKHOUSE_USER=grafana)

### grafana/datasources/clickhouse.yml
- `uid: clickhouse-fab` — dashboard.json의 모든 쿼리가 이 uid 참조
- 호스트: `clickhouse` (Docker 내부 hostname), 포트 8123

### grafana/dashboards/dashboard.json
- **템플릿 변수**: `type: "custom"` 필수 (query type + ClickHouse 쿼리는 플러그인 버전 따라 실패함)
- **쿼리 format**: `"format": "table"` 필수 (time_series 문자열은 플러그인이 거부함)
- **detectedAt 컬럼**: `detectedAt AS time` 직접 사용 (DateTime64(3) → 이미 시각 타입)

---

## 실행 순서

### 0. 사전 준비 (최초 1회)
```powershell
# Python 패키지 설치 (py -3.12 사용!)
cd C:\Users\hyohy\IdeaProjects\FabWorld

py -3.12 -m pip install fastapi uvicorn aiokafka pydantic python-dotenv httpx
# 또는 각 디렉토리에서:
py -3.12 -m pip install -r producer/requirements.txt
py -3.12 -m pip install -r processor/requirements.txt
py -3.12 -m pip install -r consumer/requirements.txt
py -3.12 -m pip install -r burst/requirements.txt
```

### 1. 인프라 기동 (Docker)
```powershell
docker-compose up -d
```
→ Kafka, ClickHouse, Grafana, Kafka-UI 자동 기동
→ ClickHouse init.sql 자동 실행 (테이블/MV 생성)
→ kafka-init 컨테이너가 토픽 자동 생성
→ Grafana 대시보드/데이터소스 자동 프로비저닝

**확인:**
- Kafka-UI: http://localhost:8080
- Grafana: http://localhost:3000 (admin/admin)
- ClickHouse ping: http://localhost:8123/ping

### 2. 터미널 4개 준비 (각각 별도 창)

**터미널 1 — HTML 서버 (file:// 대신 HTTP로 서빙)**
```powershell
py -3.12 -m http.server 5500
# 브라우저: http://localhost:5500/fab-world.html
```

**터미널 2 — FastAPI Producer**
```powershell
cd producer
py -3.12 -m uvicorn main:app --reload --port 8000
```

**터미널 3 — Stream Processor (이상 탐지 + Slack)**
```powershell
cd processor
py -3.12 main.py
```

**터미널 4 — ClickHouse Consumer**
```powershell
cd consumer
py -3.12 ch_consumer.py
```

### 3. (선택) Burst Generator — 대용량 부하 테스트
```powershell
cd burst
py -3.12 burst_generator.py --rate 5000
```
→ 5000 events/sec로 랜덤 정상 데이터 생성, Grafana Throughput 패널에서 확인

---

## Grafana 대시보드

URL: http://localhost:3000  
로그인: admin / admin  
대시보드: "FAB WORLD Monitor" (자동 로드됨)

**패널 구성:**
| 패널 | 타입 | 쿼리 대상 |
|---|---|---|
| Throughput (events/min) | timeseries | sensor_1min_agg |
| Anomaly Count (last 1h) | stat | anomaly_events (임계: 10=yellow, 50=red) |
| HIGH Severity (last 1h) | stat | anomaly_events WHERE severity='HIGH' (1개부터 red) |
| Sensor Trend — $chamberId/$sensorType | timeseries | sensor_raw (템플릿 변수 연동) |
| Recent Anomaly Events | table | anomaly_events ORDER BY detectedAt DESC LIMIT 200 |

**템플릿 변수:**
- `$chamberId`: CVD-A1, CVD-A2, PVD-B1, PVD-B2, ETCH-C1, ETCH-C2, INSP-D1, INSP-D2, CMP-E1, CMP-E2
- `$sensorType`: temp, pressure, rf_power, gas_flow, bias_voltage

---

## 트러블슈팅 전체 기록

### 1. PowerShell에서 `&&` 구문 오류
**증상:** `cd producer && uvicorn ...` → "올바른 문 구분 기호가 아닙니다"
**원인:** PowerShell은 `&&` 미지원 (bash 전용 문법)
**해결:** 명령어를 별도 줄로 분리하거나 `;` 사용

### 2. pip 명령어를 찾을 수 없음
**증상:** `pip install ...` → "'pip' 용어가 인식되지 않습니다"
**원인:** Python PATH 미설정
**해결:** `py -3.12 -m pip install ...` 사용

### 3. Python 3.14에서 aiokafka 설치 실패
**증상:** aiokafka, pydantic-core 빌드 실패 (MSVC, Rust 컴파일러 필요)
**원인:** Python 3.14는 너무 최신이라 prebuilt wheel 없음
**해결:** Python 3.12 AMD64 재설치, `py -3.12` 런처 사용

### 4. fab-world.html을 file://로 열면 fetch 차단
**증상:** 브라우저 콘솔에서 "Unsafe attempt to load URL" CORS 오류
**원인:** 브라우저 보안 정책 — file:// 출처에서 http://localhost 요청 차단
**해결:** `py -3.12 -m http.server 5500` 으로 HTTP 서버 실행, `http://localhost:5500/fab-world.html` 접속

### 5. uvicorn "Could not import module 'main'"
**증상:** 프로젝트 루트에서 `uvicorn main:app` 실행 시 ImportError
**원인:** main.py가 producer/ 폴더 안에 있는데 루트에서 실행
**해결:** `cd producer` 후 실행하거나 `py -3.12 -m uvicorn main:app --reload --port 8000`을 producer 디렉토리에서

### 6. `AIOKafkaProducer.__init__() got unexpected keyword argument 'batch_size'`
**증상:** Producer 시작 시 TypeError
**원인:** aiokafka 0.11에서 `batch_size` → `max_batch_size`로 파라미터명 변경됨
**해결:** `kafka_producer.py`, `burst_generator.py` 모두 `max_batch_size=65536`으로 수정

### 7. `RuntimeError: Compression library for lz4 not found`
**증상:** Producer에서 Kafka produce 시 런타임 에러
**원인:** lz4 Python 패키지는 설치되지만 aiokafka가 필요한 C 확장 부재
**해결:** `compression_type="lz4"` → `"gzip"` 변경 (Python 내장 gzip 사용)

### 8. `UnsupportedCodecError: Libraries for lz4 compression codec not found` (Consumer)
**증상:** ch_consumer.py에서 메시지 consume 시 에러
**원인:** `docker-compose.yml`의 kafka-init에 `--config compression.type=lz4`가 있어서 Kafka 브로커가 모든 메시지를 lz4로 재압축 → consumer가 lz4 디코딩 불가
**해결:** kafka-init 커맨드에서 `--config compression.type=lz4` 제거 → 토픽 삭제 후 재생성
```bash
docker exec fab-kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic sensor-raw
docker-compose restart kafka-init
```

### 9. Grafana "Template variable service failed"
**증상:** 대시보드 로드 시 "Cannot read properties of undefined (reading 'split')" 오류
**원인:** 변수 `type: "query"` + ClickHouse datasource 쿼리 형식을 플러그인이 처리 못함
**해결:** `type: "query"` → `type: "custom"`, 챔버 목록을 `"query"` 필드에 직접 하드코딩

### 10. Grafana ClickHouse 403 AUTHENTICATION_FAILED
**증상:** Grafana에서 모든 쿼리가 403 반환
**원인:** ClickHouse `default` 유저 변경 시도 → `users.xml` readonly 스토리지라 ALTER 불가
**해결:**
```sql
CREATE USER grafana IDENTIFIED BY 'fab123';
GRANT ALL ON fabworld.* TO grafana;
GRANT SELECT ON system.* TO grafana;
```
grafana/datasources/clickhouse.yml, .env 모두 grafana 유저로 변경

### 11. Grafana "invalid format value: time_series"
**증상:** 각 패널이 데이터를 로드하지 못함
**원인:** ClickHouse Grafana 플러그인이 `"format": "time_series"` 거부
**해결:** 모든 target에서 `"format": "table"` 사용

### 12. `[alert] SLACK_WEBHOOK_URL not set, skipping`
**증상:** 이상 탐지 후 Slack 알림이 안 감
**원인 1:** `load_dotenv()`가 `from alerter import send_slack` 이후에 호출됨
          → alerter.py 모듈 로딩 시점에 이미 `os.getenv("SLACK_WEBHOOK_URL")` 실행 → 빈값
**원인 2:** `load_dotenv("../../.env")` 상대경로가 실행 위치 따라 달라짐
**해결:**
- `load_dotenv(Path(__file__).resolve().parent.parent / ".env")` 절대경로 사용
- alerter.py에서 모듈 수준 상수 제거, `_webhook_url()` 함수로 호출 시 읽기
- processor/main.py에서 `load_dotenv` 호출을 모든 import 이전으로 이동

### 13. `Illegal types DateTime64(3) and UInt16` (Grafana)
**증상:** Recent Anomaly Events 패널에서 쿼리 에러
**원인:** `toDateTime(detectedAt / 1000)` — detectedAt이 이미 DateTime64(3)인데 UInt16으로 나누려 함
**해결:** `detectedAt AS time` 으로 직접 사용

### 14. Consumer 403 인증 실패
**증상:** ch_consumer.py에서 `[consumer] CH error 403`
**원인:** .env의 `CLICKHOUSE_USER=default`, `CLICKHOUSE_PASS=""` — default 유저 비번 없음 설정과 불일치
**해결:** .env에서 `CLICKHOUSE_USER=grafana`, `CLICKHOUSE_PASS=fab123` 로 변경

---

## 컨벤션

- Kafka 클라이언트: aiokafka만 사용 (kafka-python 혼용 금지)
- timestamp/detectedAt: Unix ms (int) — `time.time_ns() // 1_000_000`
- JSON 직렬화: `json.dumps(event).encode("utf-8")`
- 환경변수: `.env` 파일, `load_dotenv(Path(__file__).resolve().parent.parent / ".env")`
- 압축: `compression_type="gzip"` (lz4 사용 금지 — C 확장 필요)
- 에러 처리: 해커톤 단순화 → 로그 출력 후 skip (DLQ 생략)

---

## 친구 연동 포인트

- consume 대상: `sensor-raw`, `anomaly-events` 토픽
- Kafka bootstrap: `localhost:9092`
- ClickHouse HTTP: `localhost:8123`, DB: `fabworld`, User: `grafana`, Pass: `fab123`
- 스키마 상세: FAB-WORLD-DESIGN.md 3절
- `.env` 파일 직접 전달 필요 (gitignore됨)

---

## 구현 상태

- [x] fab-world.html (게임 시뮬레이터 + FastAPI 연동)
- [x] docker-compose.yml (Zookeeper, Kafka, kafka-init, kafka-ui, ClickHouse, Grafana)
- [x] clickhouse/init.sql (sensor_raw, anomaly_events, MV, grafana 유저)
- [x] producer/ (FastAPI + aiokafka + CORS)
- [x] processor/ (detector, dedup, alerter, Slack webhook)
- [x] consumer/ (ch_consumer.py — 배치 INSERT)
- [x] burst/ (burst_generator.py)
- [x] grafana/datasources/clickhouse.yml (자동 프로비저닝)
- [x] grafana/dashboards/dashboard.json (5개 패널 완성)

## 검증 완료

- [x] HTML 이상 주입 → Slack 알림 수신
- [x] Grafana Anomaly Count, HIGH Severity, Recent Anomaly Events 실데이터 표시
- [x] Burst Generator 5000 events/sec → Grafana Throughput 패널 반영

---

## 브랜치 전략

- `feature/edge` — 전체 구현 완료 (데모/검증용)
- `main` — 민섭 파트만 머지 예정 (친구 인계 기준점)