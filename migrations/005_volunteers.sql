-- Election-night volunteers: people who cover a polling place and send its
-- results in from their phone. Applied automatically by volunteers.ensure_schema.

CREATE TABLE IF NOT EXISTS volunteers (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL,
    email         TEXT    NOT NULL UNIQUE,      -- lowercased
    phone         TEXT,
    home_town     TEXT,
    affiliation   TEXT,                         -- campaign / committee they came from
    status        TEXT    NOT NULL DEFAULT 'pending',   -- pending | approved | rejected
    token         TEXT    NOT NULL UNIQUE,      -- secret for their personal link
    signup_ip     TEXT,
    admin_note    TEXT,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP,
    approved_at   TIMESTAMP,
    approved_by   INTEGER REFERENCES users(id),
    link_sent_at          TIMESTAMP,
    instructions_sent_at  TIMESTAMP,
    reminder_sent_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS volunteer_assignments (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    volunteer_id  INTEGER NOT NULL REFERENCES volunteers(id) ON DELETE CASCADE,
    municipality  TEXT    NOT NULL,             -- polling_places.municipality
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(volunteer_id, municipality)
);
CREATE INDEX IF NOT EXISTS idx_vol_assign_muni ON volunteer_assignments(municipality);

-- Photos uploaded one at a time from the phone, before the report is sent.
CREATE TABLE IF NOT EXISTS volunteer_uploads (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    volunteer_id  INTEGER NOT NULL REFERENCES volunteers(id) ON DELETE CASCADE,
    municipality  TEXT    NOT NULL,
    path          TEXT    NOT NULL,
    submission_id INTEGER,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- One sent report. It becomes an intake_messages row (source 'web',
-- external_id 'web-<id>') and goes through the same parser as email and Signal.
CREATE TABLE IF NOT EXISTS volunteer_submissions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    volunteer_id  INTEGER NOT NULL REFERENCES volunteers(id) ON DELETE CASCADE,
    municipality  TEXT    NOT NULL,
    note          TEXT,
    attachments   TEXT,                         -- JSON list of paths
    external_id   TEXT    UNIQUE,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_vol_sub_muni ON volunteer_submissions(municipality);
