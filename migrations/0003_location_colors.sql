-- Migration number: 0003 	 2024-05-23T14:00:00.000Z
CREATE TABLE IF NOT EXISTS location_colors (
    name TEXT PRIMARY KEY,
    color TEXT NOT NULL
);
