CREATE TABLE IF NOT EXISTS noc_titles (
    id SERIAL PRIMARY KEY,
    noc21_code VARCHAR(10) UNIQUE NOT NULL,
    noc21_name VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS job_postings (
    id SERIAL PRIMARY KEY,
    noc_id INT REFERENCES noc_titles(id),
    normalized_title VARCHAR(300) NOT NULL,
    vacancy_count INT,
    province VARCHAR(50),
    city VARCHAR(100),
    first_posting_date DATE,
    salary_min_hourly NUMERIC(6,2),
    salary_max_hourly NUMERIC(6,2)
);