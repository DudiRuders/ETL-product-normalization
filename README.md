# ETL Pipeline: Product Data Normalization

A robust Python-based ETL (Extract, Transform, Load) pipeline designed to clean, normalize, and standardize inconsistent retail product data from multiple disparate sources.

## 🎯 The Business Problem
In retail and e-commerce, product data often comes from various departments and external vendors. This leads to:
* Inconsistent naming conventions, typos, and abbreviations.
* Varying formats for weights and dimensions (e.g., "500g", "0.5 kg", "500 gram").
* Lack of standardization, making downstream processing, reporting, and database synchronization difficult.

## 💡 The Solution
This pipeline acts as a central data-cleansing engine. It ingests raw, "dirty" data and outputs a clean, unified dataset ready for analytics or database injection.

### Key Features:
* **Text Normalization:** Automated rules for text cleaning and standardizing naming conventions.
* **Smart Extraction:** Advanced Regex patterns to accurately extract weights, volumes, and units regardless of the input format.
* **Dictionary Mapping:** Utilizing replacement dictionaries for known abbreviations and brand names.
* **Exception Handling:** Generates a separate report for edge cases and exceptions that require manual human validation, ensuring no data is silently corrupted.
* **Flexible Output:** Processes data into clean `.csv` files or directly pushes it to a PostgreSQL database.

## 🛠️ Tech Stack
* **Language:** Python
* **Data Processing:** Pandas, Regex
* **Data Sources/Output:** CSV, XLSX, PostgreSQL

## 🔄 Workflow Architecture
1. **Extract:** Read raw data from `.csv` or `.xlsx` files originating from different departments.
2. **Transform:** - Apply base string manipulation (lowercase, trim).
   - Run Regex extraction for attributes (weights, sizes).
   - Apply dictionary replacements.
   - Flag anomalies for the exception report.
3. **Load:** Export the cleaned dataset and the exception log to standard `.csv` files or inject directly into PostgreSQL tables.

### ETL Pipeline Architecture

graph LR
    A[Raw Data CSV/XLSX] -->|Extract & Load| B(Python + Pandas)
    B --> C{Validation & Cleaning}
    C -->|Valid Records| D[(Normalized PostgreSQL)]
    C -->|Errors / Missing| E[Exception Report XLSX]
    
---
*Note: This repository serves as a structural map and portfolio showcase. Sensitive data, production credentials, and proprietary business rules have been omitted for security reasons.*
