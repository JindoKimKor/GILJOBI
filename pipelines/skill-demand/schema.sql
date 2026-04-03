-- =============================================================================
-- Skill Demand — Star Schema
-- =============================================================================

-- Dimensions
CREATE TABLE IF NOT EXISTS dim_companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(300) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_skills (
    id SERIAL PRIMARY KEY,
    name VARCHAR(300) NOT NULL UNIQUE,
    category VARCHAR(20) NOT NULL CHECK (category IN (
        'hard_skill', 'soft_skill', 'tool', 'certification'
    ))
);

CREATE TABLE IF NOT EXISTS dim_seniority (
    id SERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL UNIQUE CHECK (level IN (
        'intern', 'entry_level', 'mid_level', 'senior', 'executive'
    ))
);

-- noc_titles already exists as dim_noc (id, noc21_code, noc21_name)

-- Facts
CREATE TABLE IF NOT EXISTS fact_job_postings (
    job_id BIGINT PRIMARY KEY,
    company_id INT REFERENCES dim_companies(id),
    noc_id INT REFERENCES noc_titles(id),
    seniority_id INT REFERENCES dim_seniority(id),
    raw_title VARCHAR(300) NOT NULL,
    noc_match_score NUMERIC(4,3),
    noc_match_method VARCHAR(30),
    description TEXT
);

CREATE TABLE IF NOT EXISTS fact_job_skill_demand (
    job_id BIGINT REFERENCES fact_job_postings(job_id),
    skill_id INT REFERENCES dim_skills(id),
    PRIMARY KEY (job_id, skill_id)
);

-- Summary view (star schema join)
CREATE OR REPLACE VIEW skill_demand_summary AS
SELECT
    n.noc21_code,
    n.noc21_name,
    ds.level AS seniority,
    sk.name AS skill,
    sk.category,
    COUNT(*) AS demand_count
FROM fact_job_skill_demand f
JOIN fact_job_postings p ON f.job_id = p.job_id
JOIN dim_skills sk ON f.skill_id = sk.id
LEFT JOIN noc_titles n ON p.noc_id = n.id
LEFT JOIN dim_seniority ds ON p.seniority_id = ds.id
GROUP BY n.noc21_code, n.noc21_name, ds.level, sk.name, sk.category
ORDER BY demand_count DESC;
