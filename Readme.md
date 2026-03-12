# 🗄️ LLM Data Catalog — Fake Database Setup

A realistic e-commerce PostgreSQL database with **12 tables** and **8,000+ fake records**
to serve as the data source for the LLM-Powered Data Catalog project.

---

## 📊 Tables Created

| Table | Description | Records |
|---|---|---|
| `users` | Customer accounts | 500 |
| `addresses` | Delivery addresses | ~1,000 |
| `categories` | Product categories (parent + child) | ~20 |
| `products` | Product listings | 200 |
| `orders` | Customer orders | 1,000 |
| `order_items` | Line items per order | ~3,000 |
| `payments` | Payment transactions | ~1,000 |
| `shipments` | Delivery tracking | ~750 |
| `reviews` | Product reviews | 800 |
| `inventory` | Warehouse stock levels | ~500 |
| `coupons` | Discount codes | 50 |
| `user_events` | Clickstream / behaviour data | 5,000 |

---

## 🚀 Setup — 3 Steps

### Step 1 — Start PostgreSQL with Docker
```bash
docker-compose up -d
```
Wait ~10 seconds for PostgreSQL to be ready.

### Step 2 — Install Python dependencies
```bash
pip install -r requirements.txt
```

### Step 3 — Run the database seeder
```bash
python database/init_db.py
```

---

## 🖥️ View Your Database (pgAdmin UI)

1. Open browser → `http://localhost:5050`
2. Login: `admin@catalog.com` / `admin123`
3. Add server:
   - Host: `postgres`
   - Port: `5432`
   - Database: `ecommerce_db`
   - Username: `catalog_user`
   - Password: `catalog_pass`

---

## 🔌 Connection String
```
postgresql://catalog_user:catalog_pass@localhost:5432/ecommerce_db
```

---

## 📁 Project Structure
```
llm-data-catalog/
├── docker-compose.yml      # PostgreSQL + pgAdmin
├── requirements.txt        # Python dependencies
├── database/
│   └── init_db.py          # Table definitions + fake data seeder
└── README.md
```