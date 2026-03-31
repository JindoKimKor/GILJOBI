CREATE TABLE IF NOT EXISTS jd_postings (
    job_id BIGINT PRIMARY KEY,
    company VARCHAR(300),
    raw_title VARCHAR(300) NOT NULL,
    noc_id INT REFERENCES noc_titles(id),
    noc_match_score NUMERIC(4,3),
    noc_match_method VARCHAR(30),
    seniority VARCHAR(50) CHECK (seniority IN (
        'intern', 'entry_level', 'mid_level', 'senior', 'executive'
    )),
    description TEXT
);

CREATE TABLE IF NOT EXISTS jd_skills (
    id SERIAL PRIMARY KEY,
    jd_id BIGINT REFERENCES jd_postings(job_id),
    skill VARCHAR(100) NOT NULL,
    category VARCHAR(20) NOT NULL CHECK (category IN (
        'hard_skill', 'soft_skill', 'tool', 'certification'
    ))
);

CREATE OR REPLACE VIEW skill_demand_summary AS
SELECT
    n.noc21_code,
    n.noc21_name,
    p.seniority,
    s.skill,
    s.category,
    COUNT(*) AS demand_count
FROM jd_skills s
JOIN jd_postings p ON s.jd_id = p.job_id
LEFT JOIN noc_titles n ON p.noc_id = n.id
GROUP BY n.noc21_code, n.noc21_name, p.seniority, s.skill, s.category
ORDER BY demand_count DESC;
