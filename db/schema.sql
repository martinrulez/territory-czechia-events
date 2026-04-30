CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name    TEXT NOT NULL,
    domain          TEXT,
    industry        TEXT,
    employee_count  TEXT,
    autodesk_products TEXT,          -- comma-separated list
    account_status  TEXT DEFAULT 'prospect',
    notes           TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS contacts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      INTEGER REFERENCES accounts(id),
    first_name      TEXT,
    last_name       TEXT,
    title           TEXT,
    email           TEXT,
    phone           TEXT,
    linkedin_url    TEXT,
    source          TEXT,            -- e.g. 'salesforce', 'zoominfo', 'manual'
    notes           TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS opportunities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      INTEGER REFERENCES accounts(id),
    contact_id      INTEGER REFERENCES contacts(id),
    opp_name        TEXT,
    stage           TEXT,
    products        TEXT,            -- comma-separated
    value           REAL,
    close_date      TEXT,
    notes           TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS enrichment_cache (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lookup_type     TEXT NOT NULL,    -- 'company' or 'contact'
    lookup_key      TEXT NOT NULL,    -- domain, name+company, etc.
    result_json     TEXT NOT NULL,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(lookup_type, lookup_key)
);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_name      TEXT NOT NULL,
    event_date      TEXT,
    location        TEXT,
    url             TEXT,
    industry_focus  TEXT,           -- AEC, D&M, M&E, mixed
    relevance_score INTEGER DEFAULT 5,  -- 1-10 relevance to Autodesk
    event_type      TEXT DEFAULT 'upcoming',  -- upcoming, discovery
    notes           TEXT,
    scraped_at      TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(url)
);

CREATE TABLE IF NOT EXISTS event_companies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER NOT NULL REFERENCES events(id),
    company_name    TEXT NOT NULL,
    company_domain  TEXT,
    role            TEXT,            -- exhibitor, speaker_company, sponsor, partner, attendee
    person_name     TEXT,
    person_title    TEXT,
    person_linkedin TEXT,
    matched_account_id INTEGER REFERENCES accounts(id),
    match_confidence REAL,           -- 0.0 - 1.0
    lead_score      INTEGER,         -- 0-100
    lead_class      TEXT,            -- upsell, whitespace, displacement, new_market
    entity_type     TEXT DEFAULT 'unknown',   -- company, individual, institution, irrelevant, government, unknown
    entity_status   TEXT DEFAULT 'pending',   -- verified, unverified, pending, rejected
    reject_reason   TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS outreach_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id      INTEGER REFERENCES contacts(id),
    account_id      INTEGER REFERENCES accounts(id),
    play            TEXT,             -- e.g. 'cold_intro', 'upsell'
    subject         TEXT,
    body            TEXT,
    status          TEXT DEFAULT 'drafted',  -- drafted, sent, replied, bounced
    drafted_at      TEXT DEFAULT (datetime('now')),
    sent_at         TEXT
);
