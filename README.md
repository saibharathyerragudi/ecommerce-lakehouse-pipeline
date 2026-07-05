# E-Commerce Lakehouse Pipeline

An end-to-end data engineering project that simulates a C2C fashion marketplace pipeline using Azure Data Factory, Azure Databricks, Unity Catalog, PySpark, and Delta Lake. The project moves raw marketplace files through landing zones and Bronze/Silver/Gold layers to create a country-level Gold table for buyer engagement, seller performance, app adoption, and product activity analysis.

![E-Commerce Lakehouse Pipeline](assets/ecommerce-lakehouse-pipeline.png)

## Project At A Glance

| Area | Details |
|---|---|
| Domain | C2C fashion marketplace analytics |
| Pipeline Type | Batch data engineering pipeline |
| Cloud Stack | Azure Data Factory, ADLS Gen2, Azure Databricks |
| Lakehouse Stack | Unity Catalog, PySpark, Delta Lake |
| Data Layers | ADLS Landing Zone 1, ADLS Landing Zone 2, Bronze, Silver, Gold |
| Source Entities | Users, buyers, sellers, countries |
| Final Output | `ecom_db_bharath.gold.ecom_one_big_table` |
| Analysis Focus | Country-level marketplace activity, buyer engagement, seller performance, user behavior |

## End-To-End Flow

1. Raw C2C marketplace files land in Landing Zone 1 in ADLS Gen2.
2. ADF reads Landing Zone 1, standardizes file names and folders, and writes one raw file per entity into Landing Zone 2 in ADLS Gen2.
3. Databricks reads Landing Zone 2 through a Unity Catalog external volume.
4. The Bronze notebook writes raw managed Delta tables.
5. The Silver notebook cleans, casts, deduplicates, and enriches each table.
6. The Gold notebook aggregates users to country level and joins all entities into `ecom_db_bharath.gold.ecom_one_big_table`.

## Business Objective

The goal is to turn raw marketplace exports into a clean analytics-ready table that helps compare countries across buyer demand, seller supply, product activity, app adoption, and engagement patterns.

The pipeline answers:

- Which countries show strong buyer engagement?
- Which markets have stronger seller performance?
- How does user activity vary by country?
- Which countries are ready for dashboard-level marketplace reporting?

## Pipeline Showcase

### Landing Zone 1

Landing Zone 1 is created in ADLS Gen2 and stores raw files as received from the source system. In this repository, `data/landing-zone-1/` mirrors that ADLS landing-zone content for review.

The users data arrives as multiple batches under:

```text
data/landing-zone-1/users-chunks/
```

The user batches include schema drift:

- `chunk1.csv` to `chunk5.csv`: 24 columns, including `hasAnyApp`, `hasAndroidApp`, `hasIosApp`, and `daysSinceLastLogin`.
- `chunk6.csv` to `chunk10.csv`: 21 columns, excluding those app/login fields but including `websiteLongevity`.

This makes Landing Zone 1 the raw intake layer where file inconsistencies are preserved before standardization.

### Azure Data Factory Standardization

ADF sits between the two ADLS landing zones. It reads the raw files from Landing Zone 1, standardizes the files into consistent entity folders, and writes one clean raw file per entity into Landing Zone 2.

### Landing Zone 2

Landing Zone 2 is also created in ADLS Gen2 and contains the standardized files used by Databricks. In this repository, `data/landing-zone-2/` mirrors those standardized ADLS folders:

- `users-raw-2/users-raw.csv`
- `buyers-raw-2/buyers-raw.csv`
- `sellers-raw-2/sellers-raw.csv`
- `countries-raw-2/countries-raw.csv`

### Bronze Layer

Notebook:

```text
notebooks/01_Bronze_Layer_Unity_Catalog.py
```

The Bronze notebook creates the Unity Catalog catalog, schemas, and external volume, then reads the Landing Zone 2 folders from ADLS Gen2 and writes raw managed Delta tables:

- `ecom_db_bharath.bronze.users`
- `ecom_db_bharath.bronze.buyers`
- `ecom_db_bharath.bronze.sellers`
- `ecom_db_bharath.bronze.countries`

Bronze keeps the data close to the landed source structure and validates table creation with row-count checks.

### Silver Layer

Notebook:

```text
notebooks/02_Silver_Layer_Unity_Catalog.py
```

The Silver notebook cleans and prepares each entity for analysis.

Key transformations:

- Standardizes country, language, gender, and civility-title fields.
- Casts numeric, decimal, and boolean columns into usable types.
- Handles null values in count and metric columns.
- Deduplicates users by `identifierHash`.
- Deduplicates buyers by `country`.
- Creates user features such as `language_full`, `years_since_last_login`, `account_age_group`, `user_descriptor`, and `flag_long_title`.
- Creates buyer features such as `female_to_male_ratio`, `wishlist_to_purchase_ratio`, `high_engagement`, and `growing_female_market`.
- Creates seller features such as `seller_size_category`, `mean_products_listed_per_seller`, and `high_seller_pass_rate`.
- Creates country features such as `top_seller_ratio`, `high_female_seller_ratio`, `performance_indicator`, `high_performance`, and `activity_level`.

Silver outputs:

- `ecom_db_bharath.silver.users`
- `ecom_db_bharath.silver.buyers`
- `ecom_db_bharath.silver.sellers`
- `ecom_db_bharath.silver.countries`

### Gold Layer

Notebook:

```text
notebooks/03_Gold_Layer_Unity_Catalog.py
```

The Gold notebook creates one country-level reporting table.

It first aggregates user-level records by country, then outer-joins users, buyers, sellers, and countries into one wide table. The output uses clear source prefixes such as `Users_*`, `Buyers_*`, `Sellers_*`, and `Countries_*`.

Gold output:

```text
ecom_db_bharath.gold.ecom_one_big_table
```

The notebook also fills numeric nulls created by the outer join and performs row-count checks to validate the final table.

## Data

| File | Rows | Purpose |
|---|---:|---|
| `data/landing-zone-1/Buyers-repartition-by-country.csv` | 62 | Raw buyer metrics by country |
| `data/landing-zone-1/Comparison-of-Sellers-by-Gender-and-Country.csv` | 73 | Raw seller metrics by country and gender |
| `data/landing-zone-1/Countries-with-Top-Sellers-(Fashion-C2C).csv` | 19 | Raw seller-market summary by country |
| `data/landing-zone-1/users-chunks/chunk1.csv` | 19,783 | Raw users batch |
| `data/landing-zone-1/users-chunks/chunk2.csv` | 19,783 | Raw users batch |
| `data/landing-zone-1/users-chunks/chunk3.csv` | 19,783 | Raw users batch |
| `data/landing-zone-1/users-chunks/chunk4.csv` | 19,783 | Raw users batch |
| `data/landing-zone-1/users-chunks/chunk5.csv` | 19,781 | Raw users batch |
| `data/landing-zone-1/users-chunks/chunk6.csv` | 4,149 | Raw users batch with schema drift |
| `data/landing-zone-1/users-chunks/chunk7.csv` | 4,149 | Raw users batch with schema drift |
| `data/landing-zone-1/users-chunks/chunk8.csv` | 4,149 | Raw users batch with schema drift |
| `data/landing-zone-1/users-chunks/chunk9.csv` | 4,149 | Raw users batch with schema drift |
| `data/landing-zone-1/users-chunks/chunk10.csv` | 4,147 | Raw users batch with schema drift |
| `data/landing-zone-2/users-raw-2/users-raw.csv` | 98,913 | Standardized users file |
| `data/landing-zone-2/buyers-raw-2/buyers-raw.csv` | 62 | Standardized buyers file |
| `data/landing-zone-2/sellers-raw-2/sellers-raw.csv` | 73 | Standardized sellers file |
| `data/landing-zone-2/countries-raw-2/countries-raw.csv` | 19 | Standardized countries file |

## Skills Demonstrated

- Azure Data Factory landing-zone pipeline design
- Databricks notebook development
- Unity Catalog table and volume usage
- PySpark transformations and feature engineering
- Delta Lake Bronze/Silver/Gold modeling
- Schema drift handling
- Data type casting and null handling
- Deduplication and row-count validation
- Country-level analytical table design

## Repository Structure

```text
ecommerce-lakehouse-pipeline/
├── README.md
├── data/
│   ├── landing-zone-1/
│   │   ├── Buyers-repartition-by-country.csv
│   │   ├── Comparison-of-Sellers-by-Gender-and-Country.csv
│   │   ├── Countries-with-Top-Sellers-(Fashion-C2C).csv
│   │   └── users-chunks/
│   │       ├── chunk1.csv
│   │       ├── chunk2.csv
│   │       ├── chunk3.csv
│   │       ├── chunk4.csv
│   │       ├── chunk5.csv
│   │       ├── chunk6.csv
│   │       ├── chunk7.csv
│   │       ├── chunk8.csv
│   │       ├── chunk9.csv
│   │       └── chunk10.csv
│   └── landing-zone-2/
│       ├── buyers-raw-2/
│       │   └── buyers-raw.csv
│       ├── countries-raw-2/
│       │   └── countries-raw.csv
│       ├── sellers-raw-2/
│       │   └── sellers-raw.csv
│       └── users-raw-2/
│           └── users-raw.csv
└── notebooks/
    ├── 01_Bronze_Layer_Unity_Catalog.py
    ├── 02_Silver_Layer_Unity_Catalog.py
    └── 03_Gold_Layer_Unity_Catalog.py
```

## How To Use

1. Create or use the ADLS Gen2 Landing Zone 1 and Landing Zone 2 paths.
2. Use ADF to copy and standardize files from Landing Zone 1 into Landing Zone 2.
3. Open the notebooks in Azure Databricks.
4. Run `01_Bronze_Layer_Unity_Catalog.py`.
5. Run `02_Silver_Layer_Unity_Catalog.py`.
6. Run `03_Gold_Layer_Unity_Catalog.py`.
7. Query `ecom_db_bharath.gold.ecom_one_big_table` from Databricks SQL or connect it to a BI tool.

## Tech Stack

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Unity Catalog
- PySpark
- Delta Lake
- Databricks SQL
