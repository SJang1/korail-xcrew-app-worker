-- Migration number: 0008 	 2026-01-20T00:00:00.000Z
CREATE TABLE IF NOT EXISTS working_location_overrides (
    username TEXT NOT NULL,
    date TEXT NOT NULL,
    location TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (username, date)
);
