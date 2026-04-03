# Inventory Analytics — End to End Data Analytics Project

## Overview
An end-to-end data analytics pipeline built on inventory and vendor sales data.
Covers data ingestion, cleaning, exploratory data analysis, statistical testing
and a Vendor Sales Performance Dashboard in Power BI.

## Tech Stack
| Tool | Purpose |
|------|---------|
| Python | Core programming language |
| Pandas, NumPy | Data manipulation and analysis |
| SQLAlchemy | Database connection and ingestion |
| SQLite (inventory.db) | Local database storage |
| Matplotlib, Seaborn, Plotly | Data visualization |
| Power BI | Dashboard and reporting |
| Jupyter Notebook | EDA and analysis |

## Project Structure
```
├── Data/                   # Source CSV files (6 tables)
├── EDA PROJECT FILE/       # Jupyter notebooks — EDA and statistical testing
├── logs code/              # Python scripts — ingestion and vendor summary
├── logs/                   # Auto-generated logs
│   ├── ingestion_db.log    # Tracks ingestion time and status
│   └── vendor_summary_db.log # Tracks vendor summary pipeline
└── README.md
```

## Data Sources — 6 Tables
- Begin Inventory
- End Inventory
- Purchases
- Purchase Prices
- Sales
- Vendor Invoice

## Pipeline Steps

### Step 1 — Data Ingestion
- Loaded 6 raw CSV files into SQLite (inventory.db) using SQLAlchemy
- Automated logging tracks file name, ingestion time in ingestion_db.log

### Step 2 — Vendor Sales Summary
- Merged 6 tables using CTEs and complex SQL queries
- Performed data cleaning — null handling, type casting, whitespace removal
- Created derived KPI columns:
  - Gross Profit
  - Profit Margin %
  - Stock Turnover Ratio
  - Sales to Purchase Ratio
- Pipeline tracked in vendor_summary_db.log

### Step 3 — EDA and Statistical Testing
- Exploratory Data Analysis using Pandas and NumPy
- Statistical testing to uncover vendor performance trends
- Visualizations using Matplotlib, Seaborn and Plotly

### Step 4 — Power BI Dashboard
- Built Vendor Sales Performance Dashboard
- Key metrics: Sales, Purchases, Freight Costs, Profitability

## How to Run
1. Clone the repository
2. Place source CSV files in the `/Data` folder
3. Run ingestion script from `/logs code/`
4. Open EDA notebook from `/EDA PROJECT FILE/`

## Note
Raw data files are not uploaded due to size limitations.
