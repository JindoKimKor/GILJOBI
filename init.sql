CREATE TABLE IF NOT EXISTS job_postings (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE,
    title TEXT,
    seniority TEXT,
    seniority_removed_title TEXT,
    primary_tag TEXT,
    match_type TEXT,
);