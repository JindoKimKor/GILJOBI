# **GILJOBI** : Job Market Intelligence Platform

<p align="center">
  <img src="giljobi.png" width="300">
</p>

A data engineering capstone project that analyzes job postings from major platforms (e.g., LinkedIn, Indeed) to provide career insights using Apache Spark and LLM-based normalization.

## 📋 Project Overview

This platform processes historical job postings to help job seekers understand:

- Market share by job role
- Seniority distribution (Entry/Mid/Senior/Lead)
- Top required skills per role
- Geographic distribution of opportunities

**Key Innovation:** LLM-powered job title normalization that learns from actual data patterns.

## 🎯 Core Features

### Intelligent Title Normalization

- Consolidates title variations using LLM
  - "Software Engineer", "SWE", "SDE II" → "Software Engineer"
  - "Senior Backend Engineer", "Backend Developer" → "Backend Engineer"

### Tag-based Classification

- **Primary tags**: Base roles (e.g., "Software Engineer", "Data Scientist")
- **Secondary tags**: Specializations (e.g., "Backend", "AI/ML", "Cloud")
- Enables flexible querying: Find all "Software Engineer" roles OR all roles with "AI/ML" tag

### Two-Phase Processing Strategy

1. **Phase 1 (One-time, LLM)**: Build normalization dictionary from unique titles (~50K titles)
2. **Phase 2 (Repeatable, Rule-based)**: Apply to full dataset (1M+ rows in minutes)

**Benefits:**

- ✅ Scalable: LLM cost only for unique titles
- ✅ Data-driven: Learns from actual job market
- ✅ Fast: Production normalization uses simple lookups

## 🛠️ Technology Stack

- **Data Processing**: Apache Spark (PySpark), Pandas
- **LLM**: Ollama (llama3.2) for local development, Claude API for production
- **Storage**: SQLite (development), PostgreSQL (planned)
- **Languages**: Python 3.14
- **Environment**: Docker, Git

## 🎓 Learning Outcomes

This project demonstrates:

- **Large-scale data processing** with Apache Spark on 1M+ records
- **LLM integration** for intelligent data normalization
- **Production-grade ETL pipelines** with clear separation of concerns
- **Trade-offs**: Balancing accuracy vs. efficiency (LLM vs. rule-based approaches)
- **Real-world problem solving**: Handling messy data, inconsistent naming, and scalability

---

**Team**: 3 members | **Timeline**: 6 months | **Status**: 🚧 Active Development
