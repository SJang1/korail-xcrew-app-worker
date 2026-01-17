CREATE TABLE IF NOT EXISTS push_notifications (
    id TEXT PRIMARY KEY NOT NULL,
    username TEXT NOT NULL,
    fcmToken TEXT NOT NULL,
    trainNo TEXT NOT NULL,
    driveDate TEXT NOT NULL,
    stationName TEXT NOT NULL,
    stationIndex TEXT NOT NULL,
    createdAt TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updatedAt TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_push_notifications_username ON push_notifications (username);
CREATE INDEX IF NOT EXISTS idx_push_notifications_train_date_station ON push_notifications (trainNo, driveDate, stationName);
CREATE INDEX IF NOT EXISTS idx_push_notifications_station_index ON push_notifications (stationIndex);