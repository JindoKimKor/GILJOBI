# Azure Cloud Infrastructure Specification

**Purpose:** Deploy the GILJOBI Data Pipeline + Resume Analysis Service to Azure.
**Duration:** 7-day demo → `terraform apply` to create, `terraform destroy` to tear down.
**Budget:** Azure for Students ($100 credit)
**Region:** Canada Central (6 vCPU quota)

---

## System Context — What Lives Where

```
┌─────────────┐     ┌─────────────┐                                    ┌──────────┐
│   Vercel    │     │   Render    │                                    │ Neon DB  │
│  (Frontend) │────▶│  (Backend)  │───────────────────────────────────▶│ (Shared) │
│  Node/EJS   │     │ Spring Boot │                                    │ market-  │
└──────┬──────┘     └──────┬──────┘                                    │ trend +  │
       │                   │                                            │ skill-   │
       │            ┌──────▼─────────────── Azure ───────────────────┐ │ demand   │
       │            │                                                │ └────┬─────┘
       │            │  ┌──────────────────────────────────────────┐  │      │
       │            │  │  VM (B6as_v2: 6 vCPU, 24GB)                │  │      │
       │            │  │  Docker Compose (same as local)          │  │      │
       │            │  │                                          │  │      │
       │            │  │  Airflow (webserver, scheduler, worker)  │  │      │
       │            │  │  Spark (master, worker(s), livy)         │──│──────┘
       │            │  │  Resume Service (FastAPI + Claude CLI)   │  │
       │            │  │  Nginx (reverse proxy + SSL)             │  │
       │            │  └──────────────────────────────────────────┘  │
       │            │          │                                      │
       │            │                                                │
       │     WebSocket                                               │
       └───────────────▶ Resume Service (same VM, port 8000) ───────│──┐
                    │                    reads both DBs directly      │  │
                    └────────────────────────────────────────────────┘  │
                                                                       │
                                                         Neon DB ◀─────┘
```

| Service | Platform | Why There |
|---------|----------|-----------|
| Frontend | Vercel | Free tier, auto-deploy from GitHub |
| Backend | Render | Free tier, Spring Boot, existing setup |
| Pipeline + Spark | **Azure VM** + Docker Compose | **Same compose files as local** — Airflow + Spark + Livy, zero code change |
| Data Storage | **VM disk** (32GB) | 7-day demo — no need for persistent external storage |
| Database | Neon DB | 기존 유지, 모든 서비스에서 동일 URL 접근 |
| Resume API | **Same VM** (TBD) | FastAPI + Claude CLI + WebSocket |

### Why VM + Docker Compose (not Databricks)

Azure for Students vCPU quota (6 per region) is too small for Databricks:
- Databricks minimum VM: `Standard_DS3_v2` (4 vCPU)
- Driver + 1 worker = 8 vCPU → exceeds quota
- Single node (driver only) = no distributed processing → defeats Spark purpose
- Quota increase not available on student subscription

**VM + Docker Compose** is the pragmatic choice:
- Same Docker Compose files as local development — **zero code change**
- Spark runs in Docker containers on the VM (same as local)
- Airflow DAG code unchanged — same `@task + LivyHook` pattern
- `up.sh airflow spark-sd` works identically on VM and local

### Resume API 연결 구조

```
기존 데이터 조회:  Frontend → Backend (Render) → Neon DB
Resume 분석:      Frontend → VM Resume Service (WebSocket) → Neon DB (양쪽) + Claude CLI
```

---

## Azure Infrastructure — What Terraform Creates

```
Azure Resource Group: giljobi-rg (canadacentral)
│
├── Virtual Network: giljobi-vnet (10.0.0.0/16)
│   └── Subnet: giljobi-subnet (10.0.1.0/24)
│
├── Network Security Group: giljobi-nsg
│   ├── SSH (22)         ← admin access
│   ├── HTTP (80)        ← redirect to HTTPS
│   ├── HTTPS (443)      ← Nginx reverse proxy
│   └── Airflow (8090)   ← DAG UI
│
├── Public IP: giljobi-ip (static)
│
└── Virtual Machine: giljobi-vm
    ├── Size: Standard_B6as_v2 (6 vCPU, 24GB RAM)
    ├── OS: Ubuntu 22.04 LTS
    ├── Disk: 32GB Standard SSD
    ├── Docker + Docker Compose (cloud-init)
    └── Services (Docker Compose — same files as local):
        ├── Airflow (webserver, scheduler, worker, redis, airflow-db)
        ├── Spark (master, worker(s), livy)
        ├── Resume Service (FastAPI + Claude CLI) — TBD
        └── Nginx (reverse proxy + SSL)
```

---

## VM 내부 구조 — Docker Compose (로컬과 동일)

```
Azure VM (giljobi-vm)
│
├── /home/azureuser/giljobi/          ← git clone (auto by cloud-init)
│   ├── dags/                         ← DAG files (same as local)
│   ├── pipelines/                    ← Pipeline code (same as local)
│   ├── infra/
│   │   ├── docker-compose.airflow.yml    ← same as local
│   │   ├── docker-compose.spark-sd.yml   ← same as local
│   │   ├── up.sh / down.sh              ← same as local
│   │   └── .env                          ← auto-generated by Terraform
│   └── data/                         ← VM 로컬 디스크 (32GB)
│
├── Docker Network: giljobi-network
│   ├── Airflow (8090, 5433)
│   ├── Spark (7077, 8080, 8998)
│   └── Resume Service (8000) — TBD
│
└── Mounted:
    ├── /var/run/docker.sock           ← Docker-in-Docker (Airflow worker)
    └── /home/azureuser/.claude/       ← Claude CLI credentials
```

**핵심: Docker Compose 파일 변경 없음.** 로컬에서 `up.sh airflow` 하는 것과 VM에서 하는 것이 동일.

---

## 데이터 흐름

### Batch Pipeline

```
VM:
  Airflow → LivyHook → Spark (Docker on same VM) → Blob Storage → Neon DB
```

Same as local. Data stored on VM disk (32GB).
7-day demo — no need for persistent external storage. Final results go to Neon DB.

### Neon DB 연결

**변경 없음.** Neon DB는 외부 URL이므로 어디서든 동일한 connection string:
- 로컬 Spark → Neon DB ✅
- VM Spark → Neon DB ✅
- VM Airflow → Neon DB ✅
- Backend (Render) → Neon DB ✅

---

## Cost Estimate (7-day demo)

| Resource | Spec | 7 days |
|----------|------|--------|
| VM (B6as_v2) | 6 vCPU, 24GB | ~$30 |
| Disk (32GB) | Standard SSD | ~$1 |
| Public IP | Static | ~$0.70 |
| **Total** | | **~$32** |

$100 크레딧에 충분. 밤에 VM stop하면 더 절약 가능.

---

## Terraform 파일 구조

```
infra/azure/
├── SPEC.md                   # 이 문서
├── terraform/
│   ├── main.tf               # Provider (azurerm), resource group
│   ├── variables.tf          # VM size, region, credentials
│   ├── network.tf            # VNet, subnet, NSG, public IP
│   ├── vm.tf                 # VM + cloud-init
│   ├── outputs.tf            # VM IP, Airflow URL
│   ├── cloud-init.yaml       # VM 부팅 자동 설정
│   ├── terraform.tfvars      # 실제 값 (.gitignore)
│   └── terraform.tfvars.example
```

### Terraform Lifecycle

```bash
# 인프라 생성 (데모 시작)
cd infra/azure/terraform
terraform init
terraform apply              # ~5분 → VM + Blob Storage 생성
                             # cloud-init 자동: Docker, repo clone, blobfuse2, .env, Airflow start

# cloud-init 완료 후 자동으로:
#   - Airflow 실행 (http://<public-ip>:8090)
#   - Blob Storage 마운트 (data/ → pipeline-data container)
#   - 기존 checkpoints 접근 가능

# 데모 종료
terraform destroy            # VM 삭제, 비용 0
                             # Blob Storage 데이터는 유지 (별도 삭제 필요)
```

### cloud-init 자동 실행 내용

VM 생성 시 자동으로:
1. Docker + Docker Compose 설치
2. docker.sock 권한 (666 + systemd 영구화)
3. Git repo clone (GitHub PAT)
4. data 디렉토리 생성 + 권한 (raw, processed, checkpoints)
5. `.env` 생성 (Neon DB URL, Airflow credentials — Terraform 변수 주입)
6. Airflow 자동 시작

---

## Security

| 항목 | 대응 |
|------|------|
| SSH | Key-based only, password 비활성화 |
| Airflow UI | NSG에서 특정 IP만 허용 또는 basic auth |
| Neon DB | SSL required, connection string은 `.env`에만 |
| Claude credentials | VM 내부 volume, 외부 접근 불가 |
| GitHub PAT | Terraform sensitive variable, .env에만 |

---

## Resume API — Resume × Job Market 매칭 (TBD)

### Platform

| 항목 | 결정 |
|------|------|
| Platform | Same Azure VM (Docker container) |
| Framework | FastAPI + Claude CLI |
| 통신 | WebSocket (단계별 스트리밍) |
| 연결 | Frontend (Vercel) ↔ VM ↔ Neon DB (양쪽) + Claude CLI |

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

```
"Software engineers and designers" — 78% match (12/15 skills)

✅ 보유 스킬:        python, java, sql, aws, docker, git, rest api, ...
❌ 부족 스킬:        kubernetes, terraform, ci/cd
```

**데이터 소스:** `dim_skills` + `fact_job_skill_demand`
**LLM 역할:** Resume에서 스킬 추출

#### Section 2: 다른 NOC 유사도 TOP 5

```
1. Data scientists (21211)              — 71% match
2. Web developers and programmers (21234) — 68% match
3. Database analysts (21223)             — 65% match
```

**데이터 소스:** 전체 NOC 스킬 교집합 비율
**LLM 역할:** 없음 (DB 쿼리 + 계산)

#### Section 3: 가장 유사한 실제 Job Posting

```
🏢 Google — Senior Software Engineer

Job Description:
"We are looking for a Senior Software Engineer with experience in
 [✅ Python], [✅ distributed systems], and [❌ Kubernetes]..."

매치율: 85%
```

**데이터 소스:** `fact_job_postings` (스킬 overlap 최대)
**LLM 역할:** JD 하이라이트 마킹

#### Section 4: 강점 & Gap 분석

```
💪 강점: Backend 스택 일치율 높음 (Python, Java, SQL)
📋 Gap: Kubernetes (82% 요구), CI/CD (74%), 경력 5+ years
🎯 추천: Kubernetes 학습 우선, Data Scientist 방향도 유리
```

**LLM 역할:** 종합 분석 텍스트 생성

### 처리 흐름 (WebSocket 단계별 스트리밍)

```
Frontend                          Resume Service (VM:8000)
   │                                     │
   ├─ WS connect ──────────────────────▶ │
   ├─ send: {resume, seniority, noc} ──▶ │
   │                                     │
   │  ◀── {"stage": "extracting"}        ├─ 1. Claude CLI: resume → skills
   │  ◀── {"stage": "extracted", ...}    │
   │  ◀── {"stage": "matching_noc"}      ├─ 2. DB: target NOC 스킬 비교
   │  ◀── {"stage": "noc_match", ...}    │
   │  ◀── {"stage": "finding_similar"}   ├─ 3. DB: 전체 NOC 유사도
   │  ◀── {"stage": "similar_nocs", ...} │
   │  ◀── {"stage": "finding_posting"}   ├─ 4. DB: 가장 유사한 job posting
   │  ◀── {"stage": "best_posting", ...} │
   │  ◀── {"stage": "analyzing"}         ├─ 5. Claude CLI: 종합 분석
   │  ◀── {"stage": "analysis", ...}     │
   │  ◀── {"stage": "done"}              │
   └─ WS close ─────────────────────────┘
```

### LLM 호출 (2회)

| # | 목적 | Input | Output |
|---|------|-------|--------|
| 1 | Resume → skills 추출 | resume text | `["python", "aws", ...]` |
| 2 | 종합 분석 생성 | resume skills + DB 매칭 + best JD | strengths, gaps, recommendations, JD highlight |

매칭 계산 자체는 DB 쿼리 — LLM은 추출과 분석 텍스트 생성에만 사용.

---

## 구현 순서

```
Phase 1: Terraform 기본 인프라 (canadacentral)
  - main.tf, network.tf, vm.tf (Databricks + Blob Storage 제거)
  - terraform apply → VM 생성
  - cloud-init → Docker, repo clone, .env, Airflow 자동 시작

Phase 2: 파이프라인 동작 확인
  - Airflow UI 접근 (http://<ip>:8090)
  - DAG trigger → Spark (Docker on VM) → Neon DB 적재

Phase 3: Resume API (use case 확정됨)
  - FastAPI + Claude CLI Docker container
  - WebSocket 단계별 스트리밍
  - Neon DB 직접 조회
  - Frontend 연결

Phase 4: 데모
  - terraform apply (fresh start)
  - 전체 시연
  - terraform destroy (정리)
```

---

## Design Decision Log

### Databricks 시도 → 폐기 (2026-04-09~10)

**시도한 것:**
- Azure Databricks workspace (eastus → canadacentral)
- `DatabricksSubmitRunOperator` + auto PAT token (Terraform)
- `azure_skill_demand_dag.py` 별도 DAG

**폐기 이유:**
- Azure for Students vCPU quota: 6 per region
- Databricks 최소 VM: `Standard_DS3_v2` (4 vCPU) — DS1_v2, DS2_v2 미지원
- Driver(4) + Worker(4) = 8 vCPU → quota 초과
- Single node(4 vCPU)는 분산 처리 불가 → Spark 사용 의미 없음
- Quota 증가 요청 불가 (student subscription)

**교훈:**
- 클라우드 managed 서비스는 minimum resource 요구사항 확인 필수
- Azure for Students quota 제한은 사전 조사 필요
- VM + Docker Compose는 "적절한 도구 선택"이지 타협이 아님
