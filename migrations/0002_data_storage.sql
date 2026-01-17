-- Migration number: 0002 	 2024-05-23T13:00:00.000Z
CREATE TABLE IF NOT EXISTS schedules (
    username TEXT NOT NULL,
    date TEXT NOT NULL,
    data TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (username, date)
);

CREATE TABLE IF NOT EXISTS dia_info (
    username TEXT NOT NULL,
    date TEXT NOT NULL,
    data TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (username, date)
);
