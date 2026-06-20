# FAB WORLD — 반도체 제조 설비 이벤트 수집 및 이상 탐지 시스템
> CloudClub 시스템 스터디 해커톤

---

## 1. 시스템 개요

반도체 Fab에서 발생하는 대량의 센서 데이터를 실시간으로 수집하고, 이상 징후를 탐지해 엔지니어에게 알리는 모니터링 시스템이다.

실제 설비가 없는 해커톤 환경에서는 **HTML5 기반 FAB WORLD 게임 시뮬레이터**가 Edge Gateway 역할을 대신한다. 시뮬레이터에서 정상/이상 공정 이벤트를 발생시키고, 별도의 Burst Generator가 실제 Fab 규모(50,000 events/sec)의 부하를 재현한다.
<p align="center">
  <img width="70%" height="50%" alt="image" src="https://github.com/user-attachments/assets/dc323a27-d90b-41d4-812e-1206c98fa99b" />
</p>



### 시스템이 하는 것 (In Scope)
- 설비 센서 데이터 실시간 수집 및 버퍼링
- 임계치 기반 + 통계 기반(이동평균/3σ) 이상 탐지
- 이상 이벤트 저장 및 대시보드 시각화
- 심각도별 알림 발송 + 중복 알림 억제(dedup)
- 장애 시 Kafka offset 기반 재처리

### 시스템이 하지 않는 것 (Out of Scope)
- 실제 장비 제어
- 수율 예측 AI 모델 학습
- MES/ERP 전체 구현

---

## 2. 전체 아키텍처

```
[FAB WORLD 시뮬레이터]     [Burst Generator]
  HTML5 Canvas 게임          Python 별도 프로세스
  정상/이상 공정 클릭         5,000~50,000 events/sec
        |                          |
        | HTTP POST /events        | aiokafka produce
        ↓                          ↓
┌─────────────────────────────────────────┐
│           FastAPI Producer              │
│  - equipmentId / chamberId 태깅         │
│  - timestamp(Unix ms) 부여              │
│  - Kafka sensor-raw 토픽으로 produce    │
└─────────────────────────────────────────┘
        |
        ↓ key=chamberId, value=JSON bytes
┌─────────────────────────────────────────┐
│           Kafka Cluster                 │
│  topic: sensor-raw     (파티션 20개)    │
│  topic: anomaly-events (파티션 4개)     │
│  topic: alert-out      (파티션 2개)     │
└─────────────────────────────────────────┘
        |
        ↓ aiokafka consume
┌─────────────────────────────────────────┐
│        Python Stream Processor          │
│  - 이동평균 + 3σ 이상 탐지              │
│  - 연속 N회 초과 판정                   │
│  - dedup (60초 억제)                    │
│  - recovery 알림 판정                   │
└─────────────────────────────────────────┘
        |                    |
        ↓                    ↓
┌──────────────┐    ┌────────────────────┐
│  ClickHouse  │    │   Slack Webhook    │
│  sensor_raw  │    │   이상 알림 발송   │
│  anomaly_    │    └────────────────────┘
│  events      │
│  (MergeTree) │
└──────────────┘
        |
        ↓
┌─────────────────────────────────────────┐
│              Grafana                    │
│  - Chamber별 센서 추이 (최근 1시간)     │
│  - 이상 이벤트 목록                     │
│  - 실시간 throughput 모니터링           │
└─────────────────────────────────────────┘
```

---

## 3. 데이터 스키마

### 3-1. sensor-raw 토픽 메시지
```json
{
  "equipmentId": "EQ-001",
  "chamberId":   "CH-042",
  "sensorType":  "temp",
  "value":       327.4,
  "timestamp":   1718700000123,
  "waferId":     "W-5821",
  "lotId":       "L-2847",
  "recipeId":    "RCP-CVD-001",
  "isAnomaly":   false
}
```

### 3-2. anomaly-events 토픽 메시지
sensor-raw 필드 전체 +
```json
{
  "isAnomaly":   true,
  "anomalyType": "TEMP_SPIKE",
  "severity":    "HIGH",
  "reason":      "val=392.0 > threshold=380",
  "detectedAt":  1718700000340
}
```

### 3-3. 센서 타입 및 임계치
| sensorType    | 정상 범위   | 임계치(HIGH) | 단위  |
|--------------|------------|-------------|-------|
| temp         | 280 ~ 350  | 380         | °C    |
| pressure     | 1.8 ~ 2.5  | 3.0         | Pa    |
| rf_power     | 800 ~ 1500 | 1800        | W     |
| gas_flow     | 50 ~ 200   | 250         | sccm  |
| bias_voltage | 100 ~ 400  | 450         | V     |

---

## 4. 기술 스택 선택 근거

### Kafka — 수집 버퍼
**선택 이유:** 수집부(Producer)와 처리부(Consumer)를 분리하는 것이 핵심이다. Consumer가 죽어도 데이터는 토픽에 남아있어 offset 기반 재처리가 가능하다. 50,000 events/sec 트래픽 폭증도 Kafka가 완충한다.

**대안 대비:** RabbitMQ는 메시지 소비 후 삭제 방식이라 재처리가 어렵다. Redis Streams는 가볍지만 대용량 보관과 파티셔닝이 약하다.

**핵심 설정:**
```
linger_ms=5       # 5ms 배치 → 처리량↑, 지연 5ms 이내 유지
batch_size=65536  # 64KB → 네트워크 효율↑
compression=lz4   # 빠른 압축 → 디스크 I/O 약 4배 감소
acks=all          # 모든 replica 확인 → 유실 없음

retention:
  sensor-raw:     48시간 (재처리 버퍼 역할, 장기보관은 ClickHouse)
  anomaly-events: 7일
  alert-out:      3일
```

**파티션 전략:**
```
key = chamberId → 같은 Chamber 이벤트는 같은 파티션으로
sensor-raw: 20파티션 (1,000 chambers / 50)
```

---

### FastAPI Producer — 수집 게이트웨이
**선택 이유:** Python 생태계에서 aiokafka와 가장 자연스럽게 연동된다. async 기반이라 HTML 게임의 HTTP 요청을 받으면서 동시에 Kafka produce가 가능하다.

**역할:**
- HTML 시뮬레이터 이벤트 수신 (`POST /events`)
- 필수 필드 태깅 (equipmentId, chamberId, timestamp 등)
- Kafka `sensor-raw` 토픽으로 produce

---

### Python Stream Processor — 이상 탐지
**선택 이유 (vs Flink):** 해커톤 구현 가능성을 고려했다. Flink는 JVM 클러스터 설치와 학습 곡선이 높다. Python Consumer로도 탐지 3초, 알림 5초 요구사항을 충족한다 (실측 ~200ms).

**이상 탐지 방식 2가지:**
1. 임계치 기반: `value > threshold` → 절대 한계 초과 감지
2. 통계 기반: `|value - μ| > 3σ` → 평소 패턴 대비 이상 감지

두 방식을 함께 써서 "명백한 한계 초과"와 "미묘한 이상 패턴"을 모두 커버한다.

**dedup 로직:**
```
첫 이상 감지     → 알림 발송
60초 이내 재감지 → 스킵 (alert fatigue 방지)
60초 후 재감지   → 리마인드 알림
이상 해소        → recovery 알림
```

**메모리 영향:**
```
window 하나: 60 floats × 8B = 480B
전체 (1,000 chambers × 5 sensors): 480B × 5,000 ≈ 2.4MB → 무시 가능
```

---

### ClickHouse — 시계열 저장소
**선택 이유:** 컬럼 지향 저장으로 Float 센서값 집계 쿼리가 빠르다. MergeTree 엔진이 대량 INSERT를 배치로 처리한다. Materialized View로 INSERT 시점에 1분 집계를 자동 갱신한다.

**대안 대비:**
| | ClickHouse | TimescaleDB | InfluxDB |
|--|--|--|--|
| INSERT 속도 | ★★★★★ | ★★★ | ★★★★ |
| 집계 쿼리 | ★★★★★ | ★★★★ | ★★★ |
| SQL 지원 | 완전 | 완전 | 제한적 |

**INSERT 주의사항 (중요):**
건당 INSERT를 고빈도로 날리면 "too many parts" 에러 발생.
Consumer에서 1,000~10,000건 모아서 배치 INSERT 해야 한다.

**테이블 구조:**
```
sensor_raw       → 원본 센서 데이터, TTL 7일
anomaly_events   → 이상 이벤트, TTL 365일
sensor_1min_agg  → Materialized View, 1분 집계 (대시보드용)
```

---

### Grafana — 대시보드
**선택 이유:** ClickHouse 플러그인이 있어 SQL로 바로 연결된다. 별도 API 서버 없이 대시보드 구성 가능하다.

---

## 5. 비기능 요구사항 달성 경로

| 요구사항 | 목표 | 달성 경로 |
|---------|------|----------|
| 수집 지연 | 1초 이내 | HTTP→Kafka: ~6ms (linger_ms=5) ✓ |
| 탐지 지연 | 3초 이내 | Kafka consume→detect: ~100ms ✓ |
| 알림 지연 | 5초 이내 | detect→Slack: ~200ms ✓ |
| 장애 복구 | offset 재처리 | Consumer 재시작 → offset 되감기 ✓ |
| 확장성 | 수평 확장 | Kafka 파티션 + Consumer 인스턴스 증가 ✓ |

---

## 6. 규모 추정

### Production 가정
```
1,000 chambers × 50 sensors × 1초 주기 = 50,000 events/sec
```

### 해커톤 재현 방식
```
1,000 chambers × 5 sensors × 0.1초 주기 = 50,000 events/sec

센서 종류를 50→5개로 축소하고 발생 주기를 1초→100ms로 조정해
동일한 TPS 부하를 재현. 부하 특성은 동등하게 유지.
```

### 데이터 볼륨
```
이벤트 하나: ~200B (JSON) → LZ4 압축 후 ~80B
초당: 50,000 × 80B = 4MB/s
해커톤 데모 (2시간): ~29GB raw → 압축 후 ~8GB
```

---

## 7. 발표 시나리오

```
1. 게임 시뮬레이터에서 CVD-A1 이상 주입 클릭
2. 터미널: Kafka consumer 로그에 anomaly 이벤트 실시간 출력
3. Grafana: 해당 Chamber 센서값 그래프에 이상 구간 표시
4. Slack: 알림 수신 확인

"이게 실제 Fab 규모로 들어오면?"

5. Burst Generator 실행 → Grafana throughput 50,000/sec
6. 이상 이벤트 자동 탐지 + dedup 동작 확인
```

---

## 8. 구현 분업

### 민섭 담당
- `fab-world.html` — FAB WORLD 게임 시뮬레이터 ✓
- `producer/` — FastAPI Producer + Kafka 연동
- `burst/` — Burst Generator (별도 프로세스)
- `processor/` — Python Stream Processor (이상 탐지 + dedup)
- `docker-compose.yml` — 전체 인프라
- 친구용 연동 가이드 문서

### 친구 담당
- `consumer/` — ClickHouse INSERT Consumer
- `clickhouse/` — 테이블 스키마 + Materialized View
- `grafana/` — 대시보드 구성
- Slack Webhook 연동

### 인터페이스 (Kafka 토픽이 경계면)
```
민섭 → [sensor-raw 토픽]     → 친구 (ClickHouse INSERT)
민섭 → [anomaly-events 토픽] → 친구 (ClickHouse INSERT + Slack)
스키마: 3절 참고
```
