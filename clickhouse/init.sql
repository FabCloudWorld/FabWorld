CREATE DATABASE IF NOT EXISTS fabworld;

-- 원본 센서 데이터, TTL 7일
CREATE TABLE IF NOT EXISTS fabworld.sensor_raw (
    equipmentId String,
    chamberId   String,
    sensorType  LowCardinality(String),
    value       Float64,
    timestamp   DateTime64(3),
    waferId     String,
    lotId       String,
    recipeId    String,
    isAnomaly   Bool,
    _insertedAt DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (chamberId, sensorType, timestamp)
TTL toDateTime(timestamp) + INTERVAL 7 DAY;

-- 이상 이벤트, TTL 365일
CREATE TABLE IF NOT EXISTS fabworld.anomaly_events (
    equipmentId  String,
    chamberId    String,
    sensorType   LowCardinality(String),
    value        Float64,
    timestamp    DateTime64(3),
    waferId      String,
    lotId        String,
    recipeId     String,
    isAnomaly    Bool,
    anomalyType  LowCardinality(String),
    severity     LowCardinality(String),
    reason       String,
    detectedAt   DateTime64(3),
    _insertedAt  DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(detectedAt)
ORDER BY (chamberId, sensorType, detectedAt)
TTL toDateTime(detectedAt) + INTERVAL 365 DAY;

-- 1분 집계 (Materialized View → Grafana throughput 패널용)
CREATE TABLE IF NOT EXISTS fabworld.sensor_1min_agg (
    chamberId  String,
    sensorType LowCardinality(String),
    minute     DateTime,
    avg_value  Float64,
    max_value  Float64,
    min_value  Float64,
    cnt        UInt64
) ENGINE = SummingMergeTree()
ORDER BY (chamberId, sensorType, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS fabworld.mv_sensor_1min
TO fabworld.sensor_1min_agg AS
SELECT
    chamberId,
    sensorType,
    toStartOfMinute(timestamp) AS minute,
    avg(value)                 AS avg_value,
    max(value)                 AS max_value,
    min(value)                 AS min_value,
    count()                    AS cnt
FROM fabworld.sensor_raw
GROUP BY chamberId, sensorType, minute;