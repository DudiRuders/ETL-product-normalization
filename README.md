# ETL Pipeline: Product Data Normalization

A robust Python-based ETL (Extract, Transform, Load) pipeline designed to clean, normalize, and standardize inconsistent retail product data.

## 📊 Key Results
* **Volume:** 12,000+ records processed.
* **Speed:** ~2s execution time.
* **Automation:** 94% accuracy in data cleaning.
* **Scalability:** Handles data from 4 disparate sources.

## 🎯 The Business Problem
In retail and e-commerce, product data often comes from various departments leading to inconsistent naming conventions, typos, and varying formats for weights (e.g., "500g", "0.5 kg"). This pipeline acts as a central cleansing engine.

## 🛠️ Tech Stack
* **Language:** Python 3.9+
* **Data Processing:** Pandas, Regex
* **Database & ORM:** PostgreSQL, SQLAlchemy
* **Testing:** Pytest
* **Data Sources:** CSV, XLSX

## 🔄 Workflow Architecture
1. **Extract:** Fetching raw data from CSV/XLSX/PostgreSQL.
2. **Transform:** Normalizing text, extracting units via Regex, and flagging anomalies.
3. **Load:** Injecting clean data into a normalized PostgreSQL database or exporting to CSV.

```mermaid
graph LR
    A[Raw Data CSV/XLSX] -->|Extract & Load| B(Python + Pandas)
    B --> C{Validation & Cleaning}
    C -->|Valid Records| D[(Normalized PostgreSQL)]
    C -->|Errors / Missing| E[Exception Report XLSX]
