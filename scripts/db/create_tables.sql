-- Agents
CREATE TABLE agents (
    agent_id               VARCHAR(12) PRIMARY KEY,
    name                   VARCHAR(100),
    age                    INTEGER,
    gender                 VARCHAR(10),
    city                   VARCHAR(50),
    state                  VARCHAR(50),
    pin_code               VARCHAR(6),
    income_monthly_inr     INTEGER,
    account_number         VARCHAR(12) UNIQUE,
    account_type           VARCHAR(20),
    account_age_days       INTEGER,
    account_created_at     TIMESTAMP,
    ifsc_code              VARCHAR(11),
    kyc_tier               VARCHAR(10),
    registered_mobile      VARCHAR(10),
    registered_email       VARCHAR(100),
    device_id              VARCHAR(20),
    device_type            VARCHAR(20),
    ip_range               VARCHAR(20),
    credit_history_years   NUMERIC(4,1),
    user_type              VARCHAR(30),
    risk_tier              VARCHAR(10),
    is_mule                BOOLEAN,
    is_hawala_node         BOOLEAN,
    is_structuring         BOOLEAN,
    is_high_velocity       BOOLEAN,
    is_dormant_reactivated BOOLEAN,
    is_round_tripper       BOOLEAN,
    tx_amount_min_inr      INTEGER,
    tx_amount_max_inr      INTEGER,
    tx_freq_min_per_day    NUMERIC(4,1),
    tx_freq_max_per_day    NUMERIC(4,1),
    preferred_channels     TEXT[],
    behavior_description   TEXT,
    created_at             TIMESTAMP DEFAULT NOW()
);

-- Beneficiary Links
CREATE TABLE beneficiary_links (
    link_id           VARCHAR(12) PRIMARY KEY,
    sender_agent_id   VARCHAR(12) REFERENCES agents(agent_id),
    receiver_agent_id VARCHAR(12) REFERENCES agents(agent_id),
    link_type         VARCHAR(20),
    established_date  DATE,
    is_active         BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT NOW()
);

-- Sessions
CREATE TABLE sessions (
    session_id         VARCHAR(20) PRIMARY KEY,
    agent_id           VARCHAR(12) REFERENCES agents(agent_id),
    login_at           TIMESTAMP,
    logout_at          TIMESTAMP,
    device_id          VARCHAR(20),
    device_type        VARCHAR(20),
    ip_address         VARCHAR(45),
    ip_geo_city        VARCHAR(50),
    ip_geo_country     VARCHAR(10),
    ip_risk_score      NUMERIC(4,3),
    login_success      BOOLEAN,
    login_failed_count INTEGER DEFAULT 0,
    device_change      BOOLEAN DEFAULT FALSE,
    ip_change          BOOLEAN DEFAULT FALSE,
    is_ato_session     BOOLEAN DEFAULT FALSE,
    created_at         TIMESTAMP DEFAULT NOW()
);

-- Transactions
CREATE TABLE transactions (
    tx_id              VARCHAR(20) PRIMARY KEY,
    session_id         VARCHAR(20) REFERENCES sessions(session_id),
    sender_agent_id    VARCHAR(12) REFERENCES agents(agent_id),
    receiver_agent_id  VARCHAR(12) REFERENCES agents(agent_id),
    amount_inr         NUMERIC(12,2),
    channel            VARCHAR(10),
    mcc_code           VARCHAR(4),
    payment_type       VARCHAR(10),
    sender_city        VARCHAR(50),
    receiver_city      VARCHAR(50),
    receiver_country   VARCHAR(10) DEFAULT 'IN',
    narration          TEXT,
    timestamp          TIMESTAMP,
    amt_log            NUMERIC(8,4),
    velocity_score     NUMERIC(8,4),
    count_1h           INTEGER,
    sum_24h            NUMERIC(14,2),
    uniq_payees_24h    INTEGER,
    avg_tx_24h         NUMERIC(12,2),
    is_international   BOOLEAN DEFAULT FALSE,
    created_at         TIMESTAMP DEFAULT NOW()
);

-- Profile Change Events
CREATE TABLE profile_change_events (
    event_id         VARCHAR(20) PRIMARY KEY,
    agent_id         VARCHAR(12) REFERENCES agents(agent_id),
    session_id       VARCHAR(20) REFERENCES sessions(session_id),
    change_type      VARCHAR(20),
    old_value_masked VARCHAR(100),
    new_value_masked VARCHAR(100),
    changed_at       TIMESTAMP,
    created_at       TIMESTAMP DEFAULT NOW()
);

-- Payee Addition Events
CREATE TABLE payee_addition_events (
    event_id            VARCHAR(20) PRIMARY KEY,
    agent_id            VARCHAR(12) REFERENCES agents(agent_id),
    session_id          VARCHAR(20) REFERENCES sessions(session_id),
    new_payee_agent_id  VARCHAR(12) REFERENCES agents(agent_id),
    added_at            TIMESTAMP,
    seconds_to_first_tx INTEGER,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Ground Truth Labels
CREATE TABLE ground_truth_labels (
    tx_id                VARCHAR(20) PRIMARY KEY REFERENCES transactions(tx_id),
    is_suspicious        BOOLEAN,
    suspicion_reason     VARCHAR(30),
    suspicion_confidence NUMERIC(4,3),
    ato_signal_score     INTEGER DEFAULT 0,
    agent_risk_tier      VARCHAR(10),
    created_at           TIMESTAMP DEFAULT NOW()
);

-- Run Log
CREATE TABLE run_log (
    run_id         SERIAL PRIMARY KEY,
    script_name    VARCHAR(100),
    status         VARCHAR(20),
    rows_processed INTEGER,
    rows_written   INTEGER,
    errors         INTEGER,
    started_at     TIMESTAMP,
    finished_at    TIMESTAMP,
    notes          TEXT
);
