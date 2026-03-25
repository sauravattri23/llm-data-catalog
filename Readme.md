# 🤖 LLM-Powered Data Catalog

> An AI-powered data catalog that automatically scans databases, generates
> human-readable descriptions using GPT-4, tracks data lineage using Neo4j,
> scores data quality and serves everything via a REST API with a React search UI.

> 💡 Think of it as **Google Search + Wikipedia for a company's internal data — powered by AI.**

---

## 🏢 Real World Problem This Solves

Companies like Swiggy, Razorpay and Zepto have **thousands of database tables**.
New engineers waste weeks figuring out what tables exist, what columns mean,
and whether the data is reliable.

This project **solves that problem automatically** using LLMs.

---

## 🏗️ Architecture

```
[Data Sources: PostgreSQL / MySQL / CSV]
              ↓
    [Metadata Crawler — SQLAlchemy]
    Extracts: tables, columns, stats
              ↓
    [Apache Airflow Orchestration]
    Schedules daily pipeline runs
              ↓
    [Quality Profiler]
    Scores: completeness, uniqueness,
            freshness, validity
              ↓
    [LangChain + GPT-4 Engine]
    Auto-generates AI descriptions
    for every table and column
              ↓
         ┌────┴────┐
         ↓         ↓
   [PostgreSQL]  [Neo4j]
   (metadata)   (lineage graph)
         ↓         ↓
         └────┬────┘
              ↓
         [FastAPI]
      REST API endpoints
              ↓
       [React Search UI]
    Search catalog by keyword
              ↓
    [Grafana + Prometheus]
     Pipeline monitoring
```

---

## ✅ Build Progress

| Phase | Description | Status |
|---|---|---|
| **Phase 1** | Fake E-commerce Database (12 tables, 13K+ records) | ✅ Done |
| **Phase 2** | Metadata Crawler (112 columns, all Grade A quality) | ✅ Done |
| **Phase 3** | LLM Integration — GPT-4 auto-descriptions | ✅ Done |
| **Phase 4** | Data Lineage — Neo4j graph | 🔄 In Progress |
| **Phase 5** | Airflow Orchestration | ⏳ Coming Soon |
| **Phase 6** | FastAPI REST endpoints | ⏳ Coming Soon |
| **Phase 7** | React Search UI | ⏳ Coming Soon |
| **Phase 8** | Grafana Monitoring Dashboard | ⏳ Coming Soon |

---

## 🛠️ Full Tech Stack

| Layer | Tools |
|---|---|
| **Database** | PostgreSQL, MySQL |
| **Crawling** | SQLAlchemy Inspector, psycopg2 |
| **Data Generation** | Python Faker |
| **LLM & AI** | HuggingFace flan-t5-large, LangChain, Transformers, PyTorch |
| **Orchestration** | Apache Airflow |
| **Transformation** | dbt |
| **Lineage** | Neo4j |
| **Caching** | Redis |
| **API** | FastAPI |
| **Frontend** | React |
| **Monitoring** | Grafana, Prometheus |
| **Infrastructure** | Docker, Docker Compose |


---

## 📊 Phase 2 Results — Quality Scores

All 12 tables crawled with **zero errors**. All scored **Grade A**.

| Table | Rows | Columns | Quality Score | Grade |
|---|---|---|---|---|
| coupons | 50 | 11 | 100.0 | **A** |
| order_items | 3,033 | 8 | 100.0 | **A** |
| inventory | 402 | 9 | 100.0 | **A** |
| reviews | 800 | 9 | 100.0 | **A** |
| addresses | 1,000 | 10 | 98.75 | **A** |
| categories | 22 | 6 | 98.67 | **A** |
| orders | 1,000 | 10 | 97.16 | **A** |
| users | 500 | 11 | 97.0 | **A** |
| products | 200 | 11 | 97.0 | **A** |
| user_events | 5,000 | 10 | 96.54 | **A** |
| shipments | 573 | 8 | 95.81 | **A** |
| payments | 1,000 | 9 | 95.34 | **A** |

---

## 🤖 Phase 3 Results — AI Generated Descriptions

All 12 tables and 112 columns described automatically
using HuggingFace flan-t5-large model. Zero manual
documentation written.

| Table | AI Generated Description |
|---|---|
| `orders` | Stores all customer purchase transactions tracking the complete lifecycle from placement to delivery |
| `users` | Contains registered customer accounts including contact details, location and membership status |
| `products` | Stores product listings available for purchase including pricing, brand and inventory details |
| `payments` | Records all payment gateway transactions including method, status and transaction identifiers |
| `reviews` | Captures customer product reviews including ratings, review text and helpfulness votes |
| `user_events` | Tracks all customer clickstream behaviour including page views, searches and purchase events |

**Key Stats:**
- 12 tables described automatically
- 112 columns described automatically
- 0 lines of manual documentation written
- Local HuggingFace model — zero API cost

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- Python 3.9+
- VS Code (recommended)

### Step 1 — Clone the repo
```bash
git clone https://github.com/sauravattri23/llm-data-catalog.git
cd llm-data-catalog
```

### Step 2 — Create virtual environment
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate
```

### Step 3 — Install dependencies
```bash
pip install -r requirements.txt
```

### Step 4 — Create `.env` file
```bash
DATABASE_URL=postgresql://catalog_user:catalog_pass@localhost:5432/ecommerce_db
OPENAI_API_KEY=your_openai_api_key_here
```

### Step 5 — Start Docker services
```bash
docker-compose up -d
```

### Step 6 — Seed the database (Phase 1)
```bash
python database/init_db.py
```

### Step 7 — Run the metadata crawler (Phase 2)
```bash
cd crawler
python metadata_extractor.py
```

### Step 8 — View results in pgAdmin
```
Open: http://localhost:5050
Login: admin@catalog.com / admin123
Host: postgres | Port: 5432
Database: ecommerce_db
User: catalog_user | Pass: catalog_pass
```

---

## 📁 Project Structure

```
llm-data-catalog/
│
├── 📄 docker-compose.yml          # All services
├── 📄 requirements.txt            # Python packages
├── 📄 .env                        # Credentials (not committed)
├── 📄 .gitignore
├── 📄 README.md
│
├── 📁 database/
│   └── init_db.py                 # 12 tables + 13K fake records
│
├── 📁 crawler/
│   ├── metadata_extractor.py      # Main crawler (Phase 2)
│   ├── quality_profiler.py        # Quality scoring engine
│   └── schema_parser.py           # Type parser helper
│
├── 📁 llm_engine/                 # Phase 3 — coming soon
├── 📁 lineage/                    # Phase 4 — coming soon
├── 📁 airflow/dags/               # Phase 5 — coming soon
├── 📁 api/                        # Phase 6 — coming soon
├── 📁 frontend/                   # Phase 7 — coming soon
└── 📁 monitoring/                 # Phase 8 — coming soon
```

---

## 🗄️ Database Schema (Phase 1)

| Table | Description | Records |
|---|---|---|
| `users` | Customer accounts | 500 |
| `addresses` | Delivery addresses | ~1,000 |
| `categories` | Product categories | ~22 |
| `products` | Product listings | 200 |
| `orders` | Customer orders | 1,000 |
| `order_items` | Line items per order | ~3,033 |
| `payments` | Payment transactions | 1,000 |
| `shipments` | Delivery tracking | ~573 |
| `reviews` | Product reviews | 800 |
| `inventory` | Warehouse stock | ~402 |
| `coupons` | Discount codes | 50 |
| `user_events` | Clickstream data | 5,000 |

---

## 🔍 Catalog Tables Created (Phase 2)

| Table | Purpose |
|---|---|
| `catalog_tables` | Table-level metadata + quality scores |
| `catalog_columns` | Column-level metadata + null percentages |
| `catalog_relationships` | Foreign key relationships between tables |
| `crawl_logs` | History of every crawl run |

---


## 👨‍💻 Author

**Saurav Attri**
Data Engineering | Data Platforms | LLM Integration

[![GitHub](https://img.shields.io/badge/GitHub-Follow-black)](https://github.com/sauravattri23)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](www.linkedin.com/in/sauravattri23)

---

## ⭐ Star this repo if you find it useful!
