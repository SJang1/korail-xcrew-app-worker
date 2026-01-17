-- Migration number: 0004 	 2024-05-23T15:00:00.000Z
CREATE TABLE IF NOT EXISTS working_locations (
    username TEXT NOT NULL,
    date TEXT NOT NULL,
    location TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (username, date)
);
