# Azure Cloud Infrastructure Specification

**Purpose:** Deploy the GILJOBI Data Pipeline to Azure cloud with managed services.
**Duration:** 7-day demo → `terraform apply` to create, `terraform destroy` to tear down.
**Budget:** Azure for Students ($100 credit)

---

## System Context — What Lives Where

```
┌─────────────┐     ┌─────────────┐                                    ┌──────────┐
│   Vercel    │     │   Render    │                                    │ Neon DB  │
│  (Frontend) │────▶│  (Backend)  │───────────────────────────────────▶│ (Shared) │
│  Node/EJS   │     │ Spring Boot │                                    │ market-  │
└──────┬──────┘     └──────┬──────┘                                    │ trend +  │
       │                   │                                            │ skill-   │
       │            ┌──────▼───────────────── Azure ──────────────────┐ │ demand   │
       │            │                                                 │ └────┬─────┘
       │            │  ┌─────────────────┐   ┌────────────────────┐   │      │
       │            │  │  VM (Airflow)   │   │ Azure Databricks   │───│──────┘
       │            │  │  Docker Compose │──▶│ (Managed Spark)    │   │
       │            │  │  orchestration  │   │ job cluster        │   │
       │            │  └─────────────────┘   └────────────────────┘   │
       │            │          │                                       │
       │            │  ┌───────▼─────────┐                            │
       │            │  │  Blob Storage   │                            │
       │            │  │  raw/processed  │                            │
       │            │  └─────────────────┘                            │
       │            │                                                 │
       │     WebSocket  ┌─────────────────┐                            │
       └───────────────▶│  App Service    │───────────────────────────│──┐
                    │   │ (Resume API)    │  FastAPI + Claude CLI      │  │
                    │   │  TBD use case   │◀──────────────────────────│──┘
                    │   └─────────────────┘  reads both DBs directly  │
                    └─────────────────────────────────────────────────┘
```

| Service | Platform | Why There |
|---------|----------|-----------|
| Frontend | Vercel | Free tier, auto-deploy from GitHub |
| Backend | Render | Free tier, Spring Boot, existing setup |
| Airflow | **Azure VM** + Docker Compose | No managed Airflow on Azure, Docker Compose 재활용 |
| Spark | **Azure Databricks** | Managed Spark, 클러스터 자동 생성/종료, 포트폴리오 가치 |
| Data Storage | **Azure Blob Storage** | 클라우드 데이터 레이크, VM 독립적 영속 |
| Database | Neon DB | 기존 유지, 모든 서비스에서 동일 URL 접근 |
| Resume API | **App Service** (TBD) | **Frontend와 직접 WebSocket 연결**, Neon DB 양쪽(market-trend + skill-demand) 직접 조회, Claude CLI로 LLM 처리 |

### Resume API 연결 구조

Backend(Render)를 거치지 않고 **Frontend ↔ Resume API 직접 통신:**

```
기존 데이터 조회:  Frontend → Backend (Render) → Neon DB
Resume 분석:      Frontend → App Service (Azure) → Neon DB (양쪽) + Claude CLI
```

Resume API가 Neon DB에 직접 접근하는 이유:
- market-trend DB에서 NOC 정보 조회
- skill-demand DB에서 스킬 수요 데이터 조회
- 두 DB의 데이터를 합쳐서 LLM에 context로 전달
- Backend을 거치면 불필요한 hop — 직접 연결이 효율적

---

## 왜 이 조합인가

### Databricks (Spark 대체)

| | 로컬 (Docker Spark + Livy) | Azure Databricks |
|--|---|---|
| 클러스터 관리 | `up.sh spark` / `down.sh spark` | **자동 생성/종료** (job cluster) |
| DAG 연결 | `@task + LivyHook` | `DatabricksSubmitRunOperator` |
| Spark 코드 | `step2_noc_spark.py` | **거의 동일** — SparkSession 부분만 변경 |
| 모니터링 | Spark UI (localhost:8080) | Databricks UI (내장) |
| 비용 | 항상 실행 | 작업 시만 과금 (job cluster) |
| 포트폴리오 | "Docker로 Spark 띄움" | **"Databricks 경험"** — 채용 시장 수요 높음 |

### VM (Airflow 전용)

Azure에 managed Airflow가 없어서 VM + Docker Compose가 가장 현실적.
기존 `docker-compose.airflow.yml` 그대로 사용 — 코드 변경 없음.

### Blob Storage (데이터 레이크)

로컬 `data/raw/`, `data/processed/`를 클라우드 스토리지로 대체.
VM이 죽어도 데이터 유지. Databricks에서 직접 접근 가능.

---

## Azure Infrastructure — What Terraform Creates

```
Azure Resource Group: giljobi-rg
│
├── Virtual Network: giljobi-vnet (10.0.0.0/16)
│   └── Subnet: giljobi-subnet (10.0.1.0/24)
│
├── Network Security Group: giljobi-nsg
│   ├── SSH (22)         ← admin access
│   ├── HTTPS (443)      ← Airflow UI (Nginx)
│   └── HTTP (80)        ← redirect to HTTPS
│
├── Public IP: giljobi-ip (static)
│
├── Virtual Machine: giljobi-vm
│   ├── Size: Standard_B2s (2 vCPU, 4GB RAM) ← Airflow만이라 작아도 됨
│   ├── OS: Ubuntu 22.04 LTS
│   ├── Disk: 32GB Standard SSD
│   ├── Docker + Docker Compose (cloud-init)
│   └── Services:
│       ├── Airflow (webserver, scheduler, worker, redis, airflow-db)
│       └── Nginx (reverse proxy + SSL)
│
├── Databricks Workspace: giljobi-databricks
│   ├── Job Clusters: 자동 생성/종료 (DAG에서 제어)
│   ├── Spark Version: 14.3.x LTS
│   ├── Node Type: Standard_DS3_v2
│   └── DBFS: Spark job 코드 저장
│
├── Storage Account: giljobi-storage
│   └── Blob Container: pipeline-data
│       ├── raw/skill-demand/        (Kaggle dataset)
│       ├── processed/step1~4/       (intermediate parquet)
│       └── checkpoints/             (LLM batch checkpoints)
│
└── App Service Plan + App (TBD — Resume API)
    └── placeholder, use case 확정 후 구현
```

---

## 코드 변경 범위

### 변경 없음 (그대로)

| 파일 | 이유 |
|------|------|
| `docker-compose.airflow.yml` | VM에서 그대로 실행 |
| `pipelines/skill-demand/src/step1_select_columns.py` | Airflow worker에서 실행, 변경 없음 |
| `pipelines/skill-demand/src/step4_load.py` | Neon DB URL 동일 |
| `pipelines/market-trend/` 전체 | 기존 그대로 |

### 변경 필요

| 파일 | 현재 | Azure | 변경 내용 |
|------|------|-------|----------|
| `dags/skill_demand_dag.py` | `@task + LivyHook` | `DatabricksSubmitRunOperator` | Operator 교체 |
| `dags/skill_demand_dag.py` | `ensure_spark` / `stop_spark` | **삭제** | Databricks가 자동 관리 |
| Spark job 파일 경로 | `data/raw/...` | `wasbs://...` or `dbfs:/...` | 경로 문자열만 |
| DAG 환경 분기 | 없음 | `GILJOBI_ENV` 변수 | `local` vs `azure` 분기 추가 |

### 환경 분기 패턴

```python
# dags/skill_demand_dag.py
ENVIRONMENT = os.environ.get("GILJOBI_ENV", "local")

if ENVIRONMENT == "azure":
    # Databricks
    step2 = DatabricksSubmitRunOperator(
        new_cluster={...},
        spark_python_task={"python_file": "dbfs:/giljobi/spark/step2_noc_spark.py"}
    )
else:
    # Local Spark + Livy (기존)
    step2 = submit_livy_job(...)
```

```python
# spark/step2_noc_spark.py
ENVIRONMENT = os.environ.get("GILJOBI_ENV", "local")

if ENVIRONMENT == "azure":
    BASE_PATH = "wasbs://pipeline-data@giljobi.blob.core.windows.net"
else:
    BASE_PATH = "/opt/airflow/data"
```

---

## 데이터 흐름

### Batch Pipeline (로컬 vs Azure)

```
로컬:
  Airflow → LivyHook → Spark (Docker) → local disk → Neon DB

Azure:
  Airflow (VM) → DatabricksSubmitRunOperator → Databricks (job cluster)
    → Blob Storage (read/write) → Neon DB (final load)
    → job 끝나면 cluster 자동 종료
```

### Neon DB 연결

**변경 없음.** Neon DB는 외부 URL이므로 어디서든 동일한 JDBC URL 사용:
- 로컬 Spark → Neon DB ✅
- Databricks → Neon DB ✅
- Airflow (VM) → Neon DB ✅
- Backend (Render) → Neon DB ✅

---

## Cost Estimate (7-day demo)

| Resource | Spec | 7 days |
|----------|------|--------|
| VM (B2s) — Airflow only | 2 vCPU, 4GB | ~$10 |
| Disk (32GB) | Standard SSD | ~$1 |
| Public IP | Static | ~$0.70 |
| Databricks | Job cluster, 사용 시만 | ~$5-15 (실행 시간 의존) |
| Blob Storage | ~1GB | ~$0.02 |
| App Service (TBD) | Free or Basic | $0-$9 |
| **Total** | | **~$17-37** |

$100 크레딧에 충분. 밤에 VM stop하면 더 절약 가능.

---

## Terraform 파일 구조

```
infra/azure/
├── SPEC.md                   # 이 문서
├── terraform/
│   ├── main.tf               # Provider (azurerm), resource group
│   ├── variables.tf          # VM size, region, Databricks config
│   ├── network.tf            # VNet, subnet, NSG, public IP
│   ├── vm.tf                 # VM + cloud-init (Airflow)
│   ├── databricks.tf         # Databricks workspace
│   ├── storage.tf            # Blob Storage account + container
│   ├── outputs.tf            # VM IP, Airflow URL, Databricks URL
│   ├── cloud-init.yaml       # VM 부팅 자동 설정
│   ├── terraform.tfvars      # 실제 값 (.gitignore)
│   └── terraform.tfvars.example
```

### Terraform Lifecycle

```bash
# 인프라 생성 (데모 시작)
cd infra/azure/terraform
terraform init
terraform apply              # ~5-10분

# VM 접속 → Airflow 시작
ssh azureuser@$(terraform output -raw vm_ip)
cd giljobi && ./infra/up.sh airflow

# Airflow UI에서 DAG trigger → Databricks job 자동 실행

# 데모 종료
terraform destroy            # 전부 삭제, 비용 0
```

---

## Security

| 항목 | 대응 |
|------|------|
| SSH | Key-based only, password 비활성화 |
| Airflow UI | Nginx reverse proxy + basic auth 또는 NSG IP 제한 |
| Databricks | Azure AD 인증 (Terraform이 설정) |
| Neon DB | SSL required, connection string은 `.env`에만 |
| Blob Storage | Private access, VM + Databricks에서만 접근 |
| Claude credentials | VM 내부, 외부 접근 불가 |

---

## Resume API — Resume × Job Market 매칭

### Platform

| 항목 | 결정 |
|------|------|
| Platform | Azure App Service |
| Framework | FastAPI + Claude CLI |
| 통신 | WebSocket (단계별 스트리밍) |
| 연결 | Frontend (Vercel) ↔ App Service ↔ Neon DB (양쪽) + Claude CLI |

### Use Case

사용자가 Resume를 업로드하고 target seniority + NOC title을 선택하면, 시장 데이터 기반으로 매칭 분석을 제공.

### 사용자 입력

```
1. Resume (PDF or text) — drag & drop
2. Target seniority — 드롭다운 (intern / entry / mid / senior / executive)
3. Target NOC title — 검색 드롭다운 (510개 중 선택)
```

### 결과 구성 (4 sections)

#### Section 1: Target NOC 매치율

선택한 NOC가 요구하는 스킬 vs 내 Resume 스킬 비교.

```
"Software engineers and designers" — 78% match (12/15 skills)

✅ 보유 스킬:        python, java, sql, aws, docker, git, rest api, ...
❌ 부족 스킬:        kubernetes, terraform, ci/cd
```

**데이터 소스:** `dim_skills` + `fact_job_skill_demand` (선택한 NOC + seniority의 top skills)
**LLM 역할:** Resume에서 스킬 추출

#### Section 2: 다른 NOC 유사도 TOP 5

내 스킬셋으로 갈 수 있는 다른 직종 추천.

```
1. Data scientists (21211)              — 71% match
2. Web developers and programmers (21234) — 68% match
3. Database analysts (21223)             — 65% match
4. Computer systems developers (21230)   — 62% match
5. Information systems specialists (21222) — 58% match
```

**데이터 소스:** 모든 NOC의 요구 스킬과 내 스킬 교집합 비율 → 상위 5개
**LLM 역할:** 없음 (순수 DB 쿼리 + 계산)

#### Section 3: 가장 유사한 실제 Job Posting

유사도 가장 높은 NOC에서 실제 job posting을 가져와서 JD 하이라이트.

```
🏢 Google — Senior Software Engineer
📍 Mountain View, CA

Job Description:
"We are looking for a Senior Software Engineer with experience in
 [✅ Python], [✅ distributed systems], and [❌ Kubernetes].
 The ideal candidate has [❌ 5+ years experience] in building
 [✅ cloud-native applications] with [✅ AWS] or GCP..."

매치율: 85%
```

**데이터 소스:** `fact_job_postings` (해당 NOC + seniority, 내 스킬 overlap이 가장 큰 row)
**LLM 역할:** JD 텍스트에서 보유/부족 스킬 하이라이트 마킹

#### Section 4: 강점 & Gap 분석

LLM이 전체 context를 종합해서 생성하는 분석 텍스트.

```
💪 강점 (Strengths):
  - Backend 기술 스택이 시장 수요와 높은 일치율 (Python, Java, SQL)
  - Cloud 경험 (AWS, Docker) — 78%의 Software Engineer JD가 요구
  - REST API 설계 경험 — senior level에서 핵심 역량

📋 Gap 분석:
  - 학력: 대부분의 매칭 JD가 CS 학위 요구 — 부트캠프 출신이면 포트폴리오로 보완 필요
  - 경력: Senior level 선택했으나 JD 평균 요구 경력 5+ years — 현재 경력 확인 필요
  - 핵심 스킬 부족:
    • Kubernetes — 82%의 Senior SWE JD가 요구, 가장 시급
    • CI/CD — 74%가 요구
    • Terraform — 45%가 요구, 클라우드 역할에서 증가 추세

🎯 추천:
  - Kubernetes 학습 우선순위 높음 (시장 수요 1위 부족 스킬)
  - Data Scientist (71% 매치) 방향도 현재 스킬셋으로 유리
```

**LLM 역할:** Resume 스킬 + DB 매칭 데이터 + JD 텍스트를 context로 받아서 종합 분석 생성

### 처리 흐름 (WebSocket 단계별 스트리밍)

```
Frontend                          App Service (Resume API)
   │                                     │
   ├─ WS connect ──────────────────────▶ │
   ├─ send: {resume, seniority, noc} ──▶ │
   │                                     │
   │  ◀── {"stage": "extracting"}        ├─ 1. Claude CLI: resume → skills 추출
   │  ◀── {"stage": "extracted",         │
   │        "skills": [...]}             │
   │                                     │
   │  ◀── {"stage": "matching_noc"}      ├─ 2. DB 쿼리: target NOC 스킬 비교
   │  ◀── {"stage": "noc_match",         │     → dim_skills + fact_job_skill_demand
   │        "match_rate": 0.78,          │
   │        "matched": [...],            │
   │        "missing": [...]}            │
   │                                     │
   │  ◀── {"stage": "finding_similar"}   ├─ 3. DB 쿼리: 전체 NOC 유사도 계산
   │  ◀── {"stage": "similar_nocs",      │
   │        "top5": [...]}               │
   │                                     │
   │  ◀── {"stage": "finding_posting"}   ├─ 4. DB 쿼리: 가장 유사한 job posting
   │  ◀── {"stage": "best_posting",      │     → fact_job_postings
   │        "company": "Google",         │
   │        "title": "Senior SWE",       │
   │        "jd_highlighted": "..."}     │
   │                                     │
   │  ◀── {"stage": "analyzing"}         ├─ 5. Claude CLI: 종합 분석 생성
   │  ◀── {"stage": "analysis",          │     (skills + DB 매칭 + JD를 context로)
   │        "strengths": [...],          │
   │        "gaps": [...],               │
   │        "recommendations": "..."}    │
   │                                     │
   │  ◀── {"stage": "done"}              │
   └─ WS close ─────────────────────────┘
```

### API Endpoint

```
WebSocket: wss://giljobi-resume.azurewebsites.net/ws/analyze

Message (client → server):
{
  "resume_text": "...",              // or base64 PDF
  "target_seniority": "senior",
  "target_noc_code": "21231"
}

Messages (server → client): 6 stages streamed sequentially (위 흐름 참조)
```

### DB 쿼리 (Resume API가 직접 실행)

```sql
-- Section 1: Target NOC 요구 스킬 (seniority 필터)
SELECT sk.name, sk.category, COUNT(*) as demand_count
FROM fact_job_skill_demand f
JOIN fact_job_postings p ON f.job_id = p.job_id
JOIN dim_skills sk ON f.skill_id = sk.id
JOIN dim_seniority ds ON p.seniority_id = ds.id
WHERE p.noc_id = (SELECT id FROM noc_titles WHERE noc21_code = :noc_code)
  AND ds.level = :seniority
GROUP BY sk.name, sk.category
ORDER BY demand_count DESC
LIMIT 30;

-- Section 2: 전체 NOC 유사도 (내 스킬과 교집합)
SELECT n.noc21_code, n.noc21_name,
       COUNT(*) as overlap,
       COUNT(*) * 1.0 / NULLIF(total.cnt, 0) as match_rate
FROM fact_job_skill_demand f
JOIN fact_job_postings p ON f.job_id = p.job_id
JOIN dim_skills sk ON f.skill_id = sk.id
JOIN noc_titles n ON p.noc_id = n.id
LEFT JOIN (
    SELECT p2.noc_id, COUNT(DISTINCT f2.skill_id) as cnt
    FROM fact_job_skill_demand f2
    JOIN fact_job_postings p2 ON f2.job_id = p2.job_id
    GROUP BY p2.noc_id
) total ON total.noc_id = p.noc_id
WHERE sk.name IN (:user_skills)
GROUP BY n.noc21_code, n.noc21_name, total.cnt
ORDER BY match_rate DESC
LIMIT 5;

-- Section 3: 가장 유사한 job posting
SELECT p.job_id, c.name as company, p.raw_title, p.description,
       COUNT(*) as skill_overlap
FROM fact_job_postings p
JOIN dim_companies c ON p.company_id = c.id
JOIN fact_job_skill_demand f ON f.job_id = p.job_id
JOIN dim_skills sk ON f.skill_id = sk.id
WHERE p.noc_id = (SELECT id FROM noc_titles WHERE noc21_code = :best_noc_code)
  AND sk.name IN (:user_skills)
GROUP BY p.job_id, c.name, p.raw_title, p.description
ORDER BY skill_overlap DESC
LIMIT 1;
```

### LLM 호출 (2회)

| # | 목적 | Input | Output |
|---|------|-------|--------|
| 1 | Resume → skills 추출 | resume text | `["python", "aws", ...]` |
| 2 | 종합 분석 생성 | resume skills + DB 매칭 결과 + best JD | strengths, gaps, recommendations, JD highlight |

매칭 계산 자체는 DB 쿼리로 처리 — LLM은 추출과 분석 텍스트 생성에만 사용.

---

## 구현 순서

```
Phase 1: Terraform 기본 인프라
  - main.tf, network.tf, vm.tf, storage.tf, databricks.tf
  - terraform apply → VM + Databricks workspace 생성 확인

Phase 2: Airflow 배포 (VM)
  - cloud-init → Docker 자동 설치
  - Git repo clone, up.sh airflow
  - Airflow UI 접근 확인

Phase 3: Databricks 연동
  - Spark job 코드를 DBFS에 업로드
  - DAG에 DatabricksSubmitRunOperator 추가 (환경 분기)
  - DAG trigger → Databricks job cluster 생성 → 실행 → 자동 종료 확인

Phase 4: 데이터 경로 전환
  - Blob Storage mount (blobfuse2 on VM)
  - Spark job에서 wasbs:// 경로 사용
  - 전체 파이프라인 end-to-end 테스트

Phase 5: Resume API (use case 확정 후)
  - App Service + FastAPI + Claude CLI
  - WebSocket 단계별 스트리밍
  - Backend 연결

Phase 6: 데모
  - terraform destroy → terraform apply (clean start)
  - 전체 시연
  - terraform destroy (정리)
```
