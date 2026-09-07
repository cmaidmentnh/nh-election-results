-- Results intake: raw messages from email / Signal, and the individual
-- vote lines parsed out of them.
--
-- Every inbound message is stored verbatim before anything is parsed, so a
-- bad parse can always be replayed against the original once it is fixed.

CREATE TABLE IF NOT EXISTS intake_messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT    NOT NULL,              -- 'email' | 'signal'
    external_id   TEXT    NOT NULL,              -- Message-ID, or signal author+timestamp
    sender        TEXT,                          -- email address or Signal name/number
    subject       TEXT,
    body          TEXT,
    attachments   TEXT,                          -- JSON array of stored file paths
    received_at   TIMESTAMP,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status        TEXT    NOT NULL DEFAULT 'new',
                  -- new | applied | queued | partial | ignored | error
    parse_json    TEXT,                          -- raw model output, for debugging
    error         TEXT,
    UNIQUE(source, external_id)
);

CREATE INDEX IF NOT EXISTS idx_intake_messages_status
    ON intake_messages(status, created_at);

CREATE TABLE IF NOT EXISTS intake_items (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id     INTEGER NOT NULL REFERENCES intake_messages(id),
    kind           TEXT    NOT NULL DEFAULT 'result',   -- result | writein | ballots
    municipality   TEXT,                                -- resolved canonical name
    municipality_text TEXT,                             -- what the reporter wrote
    election_id    INTEGER,
    race_id        INTEGER,
    race_text      TEXT,
    candidate_id   INTEGER,
    candidate_text TEXT,
    votes          INTEGER,
    old_votes      INTEGER,                             -- value already on file, if any
    confidence     REAL,
    status         TEXT    NOT NULL DEFAULT 'pending',
                   -- pending | applied | rejected | superseded
    reason         TEXT,                                -- why it was held for review
    applied_at     TIMESTAMP,
    reviewed_by    INTEGER REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_intake_items_status
    ON intake_items(status, municipality);
CREATE INDEX IF NOT EXISTS idx_intake_items_message
    ON intake_items(message_id);
