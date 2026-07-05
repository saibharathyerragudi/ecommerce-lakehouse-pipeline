# E-Commerce Lakehouse Pipeline

End-to-end data engineering project that takes raw marketplace files through Azure Data Factory landing zones, Azure Databricks, Unity Catalog, and Delta Lake medallion layers. The final output is a country-level Gold table designed for SQL analytics and dashboard-ready reporting.

## What This Project Builds

- Raw ecommerce data is organized into two ADLS-style landing zones.
- Azure Data Factory standardizes source files into consistent raw folders.
- Databricks reads the landed files through a Unity Catalog external volume.
- Bronze Delta tables preserve the raw entity structure.
- Silver Delta tables clean, type-cast, standardize, enrich, and deduplicate data.
- Gold combines user, buyer, seller, and country metrics into one analytics-serving table.

## Architecture

```text
Source CSV files
      |
      v
landing-zone-1
Raw source files with original names and chunked user batches
      |
      v
Azure Data Factory copy flow
Standardizes file names and entity-level folders
      |
      v
landing-zone-2
users-raw-2, buyers-raw-2, sellers-raw-2, countries-raw-2
      |
      v
Unity Catalog external volume
ecom_db_bharath.raw.raw_files
      |
      v
Databricks + Delta Lake
Bronze -> Silver -> Gold
      |
      v
Gold serving table
ecom_db_bharath.gold.ecom_one_big_table
```

## Data Sources

| Entity | Grain | Role in pipeline |
| --- | --- | --- |
| `users` | One row per user | User demographics, account history, app usage, product activity |
| `buyers` | One row per country | Country-level buyer totals, gender mix, wishlist and purchase behavior |
| `sellers` | One row per country and gender | Seller count, seller performance, product listing and sold metrics |
| `countries` | One row per country | Country-level marketplace activity and top-seller indicators |

## Landing Zones

```text
data/
├── landing-zone-1/
│   ├── users-chunks/
│   │   ├── chunk1.csv ... chunk10.csv
│   ├── Buyers-repartition-by-country.csv
│   ├── Comparison-of-Sellers-by-Gender-and-Country.csv
│   └── Countries-with-Top-Sellers-(Fashion-C2C).csv
└── landing-zone-2/
    ├── users-raw-2/users-raw.csv
    ├── buyers-raw-2/buyers-raw.csv
    ├── sellers-raw-2/sellers-raw.csv
    └── countries-raw-2/countries-raw.csv
```

`landing-zone-1` represents the as-received source files. The users feed arrives in ten chunks, which models a common batch-ingestion pattern where a source system exports data in partitions instead of one clean file.

`landing-zone-2` represents the standardized output consumed by Databricks. In the deployed workflow, the folders are read from ADLS Gen2 through a Unity Catalog external volume.

## Notebooks

Run the notebooks in this order:

1. `notebooks/01_Bronze_Layer_Unity_Catalog.py`
2. `notebooks/02_Silver_Layer_Unity_Catalog.py`
3. `notebooks/03_Gold_Layer_Unity_Catalog.py`

## Bronze Layer

The Bronze notebook creates Unity Catalog objects and writes raw landed files into managed Delta tables:

- `ecom_db_bharath.bronze.users`
- `ecom_db_bharath.bronze.buyers`
- `ecom_db_bharath.bronze.sellers`
- `ecom_db_bharath.bronze.countries`

Bronze keeps the data close to its landed structure so the raw-to-curated lineage is easy to audit.

## Silver Layer

The Silver notebook prepares each entity for analytics:

- Normalizes country and text fields.
- Casts numeric, boolean, and date-like fields into usable types.
- Deduplicates user and country-level records.
- Handles nulls in count and ratio columns.
- Adds derived fields such as account age groups, engagement flags, seller size categories, and market performance indicators.

## Gold Layer

The Gold notebook builds `ecom_db_bharath.gold.ecom_one_big_table`.

Key design choices:

- Aggregates user-level data to country grain before joining.
- Outer joins country-level users, buyers, sellers, and country market metrics.
- Uses explicit source-prefixed column names such as `Users_TotalUsers`, `Buyers_Total`, and `Sellers_Total`.
- Fills numeric nulls introduced by outer joins.
- Compares Gold row count against source country counts to catch unexpected row loss or join fan-out.

## How To Run

Prerequisites:

- Azure Data Factory pipeline that lands source files into ADLS Gen2.
- Azure Databricks workspace with Unity Catalog enabled.
- Unity Catalog storage credential and external location for the landing-zone storage path.
- Permission to create catalogs, schemas, volumes, and managed Delta tables.

Databricks run order:

```text
01_Bronze_Layer_Unity_Catalog.py
02_Silver_Layer_Unity_Catalog.py
03_Gold_Layer_Unity_Catalog.py
```

The notebooks currently use:

```text
Catalog: ecom_db_bharath
Raw volume: ecom_db_bharath.raw.raw_files
Gold table: ecom_db_bharath.gold.ecom_one_big_table
```

Update the catalog, volume, and ADLS path values if running in a different workspace.

## Repository Structure

```text
ecommerce-lakehouse-pipeline/
├── README.md
├── notebooks/
│   ├── 01_Bronze_Layer_Unity_Catalog.py
│   ├── 02_Silver_Layer_Unity_Catalog.py
│   └── 03_Gold_Layer_Unity_Catalog.py
└── data/
    ├── landing-zone-1/
    └── landing-zone-2/
```

## Tech Stack

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Unity Catalog
- PySpark
- Delta Lake
- Databricks SQL

## Project Outcome

This project demonstrates a lakehouse pipeline pattern for moving messy marketplace files into governed, queryable Delta tables. The final Gold table supports country-level analysis of marketplace participation, buyer engagement, seller performance, app adoption, and product activity.
