
-- Users Table
CREATE TABLE IF NOT EXISTS user (
    id SERIAL PRIMARY KEY,
    email VARCHAR NOT NULL UNIQUE,
    hashed_password VARCHAR NOT NULL,
    role VARCHAR DEFAULT 'user',
    is_active BOOLEAN DEFAULT TRUE
);

-- Processing Purposes (ROPA)
CREATE TABLE IF NOT EXISTS processingpurpose (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    description VARCHAR,
    legal_basis VARCHAR
);

-- Scan Configurations
CREATE TABLE IF NOT EXISTS scanconfig (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    target_type VARCHAR NOT NULL,
    target_path VARCHAR NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    tags VARCHAR,
    last_scan_at TIMESTAMP,
    purpose_id INTEGER REFERENCES processingpurpose(id)
);

-- Audit Logs
CREATE TABLE IF NOT EXISTS auditlog (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id INTEGER REFERENCES user(id),
    user_email VARCHAR,
    action VARCHAR NOT NULL,
    endpoint VARCHAR,
    ip_address VARCHAR,
    details VARCHAR
);

-- Scan Rules
CREATE TABLE IF NOT EXISTS scanrule (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    rule_type VARCHAR NOT NULL,
    pattern VARCHAR NOT NULL,
    score FLOAT DEFAULT 0.5,
    entity_type VARCHAR NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    context_keywords VARCHAR
);

-- Scan Results
CREATE TABLE IF NOT EXISTS scanresult (
    id SERIAL PRIMARY KEY,
    config_id INTEGER REFERENCES scanconfig(id),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    item_name VARCHAR NOT NULL,
    item_location VARCHAR NOT NULL,
    pii_type VARCHAR NOT NULL,
    count INTEGER NOT NULL,
    confidence_score FLOAT DEFAULT 0.0,
    sample_data VARCHAR,
    location_metadata VARCHAR,
    is_encrypted BOOLEAN DEFAULT FALSE
);

-- Detected Data (Inventory)
CREATE TABLE IF NOT EXISTS detecteddata (
    id SERIAL PRIMARY KEY,
    source VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    pii_type VARCHAR NOT NULL,
    sensitivity VARCHAR NOT NULL,
    purpose VARCHAR,
    confidence_score FLOAT NOT NULL,
    status VARCHAR DEFAULT 'Active',
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
