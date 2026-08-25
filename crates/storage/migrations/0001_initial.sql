CREATE TABLE IF NOT EXISTS candles (
    coin      TEXT    NOT NULL,
    timeframe TEXT    NOT NULL,
    open_time INTEGER NOT NULL,
    open      TEXT    NOT NULL,
    high      TEXT    NOT NULL,
    low       TEXT    NOT NULL,
    close     TEXT    NOT NULL,
    volume    TEXT    NOT NULL,
    PRIMARY KEY (coin, timeframe, open_time)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS lists (
    name TEXT PRIMARY KEY
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS list_members (
    list TEXT NOT NULL,
    coin TEXT NOT NULL,
    PRIMARY KEY (list, coin)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS logos (
    coin TEXT PRIMARY KEY,
    svg  TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS signals (
    id         TEXT NOT NULL CHECK (length(id) > 0),
    name       TEXT NOT NULL COLLATE NOCASE UNIQUE CHECK (length(trim(name)) > 0),
    definition TEXT NOT NULL,
    PRIMARY KEY (id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS indicator_presets (
    id           TEXT NOT NULL CHECK (length(id) > 0),
    indicator_id TEXT NOT NULL CHECK (length(indicator_id) > 0),
    name         TEXT NOT NULL COLLATE NOCASE CHECK (length(trim(name)) > 0),
    params       TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (indicator_id, name)
) WITHOUT ROWID;
