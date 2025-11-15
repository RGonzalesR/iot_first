\c iot_db;

-- Tabela de leituras brutas processadas
CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    sensor_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    normalized_value DOUBLE PRECISION,
    status VARCHAR(20),
    location VARCHAR(200),
    manufacturer VARCHAR(200),
    model VARCHAR(100),
    unit VARCHAR(20),
    high_alert BOOLEAN DEFAULT FALSE,
    low_alert BOOLEAN DEFAULT FALSE,
    processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de agregações
CREATE TABLE IF NOT EXISTS sensor_aggregations (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    sensor_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    location VARCHAR(200),
    manufacturer VARCHAR(200),
    avg_value DOUBLE PRECISION,
    min_value_reading DOUBLE PRECISION,
    max_value_reading DOUBLE PRECISION,
    stddev_value DOUBLE PRECISION,
    reading_count BIGINT,
    max_normalized DOUBLE PRECISION,
    high_alert_count BIGINT,
    low_alert_count BIGINT,
    aggregation_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(window_start, window_end, sensor_id)
);

-- Mensagens inválidas (Dead Letter Queue)
CREATE TABLE IF NOT EXISTS sensor_errors (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    raw_value TEXT NOT NULL,
    error_type TEXT,
    error_message TEXT,
    kafka_topic TEXT,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP
);

-- Índices para performance
CREATE INDEX idx_sensor_readings_timestamp ON sensor_readings(timestamp);
CREATE INDEX idx_sensor_readings_sensor_id ON sensor_readings(sensor_id);
CREATE INDEX idx_sensor_readings_type ON sensor_readings(sensor_type);
CREATE INDEX idx_sensor_readings_location ON sensor_readings(location);

CREATE INDEX idx_sensor_agg_window ON sensor_aggregations(window_start, window_end);
CREATE INDEX idx_sensor_agg_sensor_id ON sensor_aggregations(sensor_id);
CREATE INDEX idx_sensor_agg_type ON sensor_aggregations(sensor_type);

-- Views para análise
CREATE OR REPLACE VIEW latest_sensor_status AS
SELECT DISTINCT ON (sensor_id)
    sensor_id,
    sensor_type,
    location,
    manufacturer,
    value,
    normalized_value,
    high_alert,
    low_alert,
    timestamp
FROM sensor_readings
ORDER BY sensor_id, timestamp DESC;

CREATE OR REPLACE VIEW hourly_sensor_summary AS
SELECT 
    date_trunc('hour', window_start) as hour,
    sensor_type,
    location,
    COUNT(*) as total_readings,
    AVG(avg_value) as avg_value,
    SUM(high_alert_count) as total_high_alerts,
    SUM(low_alert_count) as total_low_alerts
FROM sensor_aggregations
GROUP BY date_trunc('hour', window_start), sensor_type, location
ORDER BY hour DESC;

-- Permissões
GRANT ALL PRIVILEGES ON DATABASE iot_db TO iot_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO iot_user;
