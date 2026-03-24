CREATE TABLE users (
    id VARCHAR(36) PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(120) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE analysis_reports (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    dataset_name VARCHAR(180) NOT NULL,
    source_type VARCHAR(30) NOT NULL,
    target_column VARCHAR(120),
    status VARCHAR(20) NOT NULL DEFAULT 'completed',
    row_count INTEGER NOT NULL,
    column_count INTEGER NOT NULL,
    share_token VARCHAR(64) NOT NULL UNIQUE,
    report_payload JSONB NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ix_analysis_reports_user_created_at
    ON analysis_reports (user_id, created_at DESC);

CREATE INDEX ix_users_email ON users (email);
CREATE INDEX ix_analysis_reports_user_id ON analysis_reports (user_id);
CREATE INDEX ix_analysis_reports_share_token ON analysis_reports (share_token);

