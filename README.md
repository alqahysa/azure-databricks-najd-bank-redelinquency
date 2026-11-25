# Najd National Bank: 30-Day Re-Delinquency Analytics on Azure Databricks

An end-to-end data engineering project for a fictional Saudi retail bank. It ingests core-banking extracts, builds a Kimball star schema in a medallion lakehouse (bronze → silver → gold), flags customers who fall behind again within 30 days of curing a missed installment, and serves the results through Genie, an AI/BI dashboard and Databricks One. Deployment is automated with Databricks Asset Bundles and GitHub Actions.

All data in this repo is synthetic.

## Business problem

Customers who become current again after a missed installment often miss again within 30 days ("30-day re-delinquency"). This raises expected credit losses and provisions, strains collections teams, and draws regulatory attention from SAMA.

Goals:

1. Identify 30-day re-delinquencies: a missed installment whose due date falls 0–30 days after the same customer cured the previous delinquency on the same product.
2. Highlight the products and customer segments that drive most of them.
3. Compare performance across branches, relationship managers and regions.

## Architecture

```text
Core banking (T24 / Finacle)  ->  Azure Data Factory (nightly)  ->  ADLS Gen2  data/staging/<entity>/
                                     (simulated here by uploading the CSVs in raw_data/)
                                                     |
                                     Auto Loader (bronze)  ->  MERGE + PII masking (silver)  ->  KPIs (gold)
                                                     |
                                     Genie space  ·  AI/BI dashboard  ·  Databricks One
```

- **Storage:** ADLS Gen2 with containers `metastore-root`, `data`, `catalog-location`
- **Governance:** Unity Catalog, catalog `banking`, schemas `bronze`, `silver`, `gold`
- **Compute:** serverless (notebooks and jobs), serverless SQL warehouse
- **Orchestration:** Lakeflow Job `banking_end_to_end_job`
- **CI/CD:** Databricks Asset Bundle `banking-end-to-end-project` + GitHub Actions

## Data model

Grain: one row per installment delinquency event.

| Layer | Table | Description |
| --- | --- | --- |
| Bronze | `cbs_customer`, `cbs_branch`, `cbs_product`, `cbs_repayment` | Raw core-banking feeds (`cbs` = Core Banking System), append only |
| Silver | `dim_customer` | Customers, National ID / Iqama and names masked (PDPL) |
| Silver | `dim_branch` | Branches with city and Saudi administrative region |
| Silver | `dim_product` | Products with Shariah structure (Murabaha / Tawarruq / Ijara) |
| Silver | `fact_repayment` | Wide fact: delinquency events joined to all dimensions |
| Gold | `fact_redelinquency_event` | Each event with its 30-day re-delinquency flag |
| Gold | `agg_redelinquency_branch_product` | KPIs by branch × product: events, re-delinquencies, rate (%), total and average SAR |

Naming: lowercase snake_case, singular entities, three-part names `banking.<layer>.<table>`. Column suffixes: `_id` / `_code` keys, `_date` dates, `_sar` amounts, `_flag` 0/1 indicators.

## Repository structure

```text
azure-databricks-najd-bank-redelinquency/
├── bronze/
│   ├── customers_raw_data_ingestion.py
│   ├── branches_raw_data_ingestion.py
│   ├── products_raw_data_ingestion.py
│   └── repayments_raw_data_ingestion.py
├── silver/
│   ├── dim_customer_ingestion.py
│   ├── dim_branch_ingestion.py
│   ├── dim_product_ingestion.py
│   └── fact_repayment_ingestion.py
├── gold/
│   └── redelinquency_analysis.py
├── raw_data/
│   ├── customer_raw.csv        # 20 customers
│   ├── branch_raw.csv          # 15 branches
│   ├── branch_raw_2.csv        # 2nd delivery: new branch BR016
│   ├── product_raw.csv         # 15 products
│   ├── product_raw_2.csv       # 2nd delivery: HF02 description change
│   └── repayment_raw.csv       # 30 delinquency events
├── resources/
│   └── job.yml                 # Lakeflow Job (relative notebook paths)
├── databricks.yml              # Asset Bundle: dev and prod targets
└── .github/workflows/
    ├── deploy-dev.yml          # on push to dev
    └── deploy-prod.yml         # on push to main
```

## Pipeline logic

**Bronze:** Auto Loader (`cloudFiles`, CSV, header, inferred types with schema hints, one file per trigger, `availableNow`) appends each staging feed to its bronze table.

**Silver:** each notebook reads bronze with `spark.readStream.table`, then uses `foreachBatch` to deduplicate on the business key, apply a `transform` function (typing, cleaning, PII masking, `load_timestamp`) and `MERGE` into the target: update when matched, insert when not. The target is created empty once with `mode("ignore")`, so a rerun can never overwrite it. The fact notebook joins all three dimensions into one wide table and merges on `event_id`.

**Gold:** a batch read of the silver fact, then:

```python
w = Window.partitionBy("customer_id", "product_code").orderBy("due_date")
prev_cure_date       = lag("cure_date").over(w)
days_since_last_cure = datediff("due_date", prev_cure_date)
redelinquency_30d_flag = 1 if 0 <= days_since_last_cure <= 30 else 0
```

The result is aggregated by branch × product into the KPI table.

## Prerequisites

- An Azure subscription (pay-as-you-go or free account) and an Azure Databricks **Premium** workspace
- ADLS Gen2 storage account (hierarchical namespace on), Access Connector for Azure Databricks with **Storage Blob Data Contributor** on the storage account
- Unity Catalog metastore assigned to the workspace, with a storage credential and external locations for the `data` and `catalog-location` containers
- Databricks CLI (`databricks -v`) and a personal access token
- A GitHub repository with `main` and `dev` branches

**Data residency:** choose an Azure region that meets your organization's SAMA / NDMO requirements, and check current Azure region availability for Saudi Arabia rather than assuming it.

## Setup

1. Create the catalog and schemas:

   ```sql
   CREATE CATALOG IF NOT EXISTS banking
     MANAGED LOCATION 'abfss://catalog-location@<storage-account>.dfs.core.windows.net/';
   CREATE SCHEMA IF NOT EXISTS banking.bronze;
   CREATE SCHEMA IF NOT EXISTS banking.silver;
   CREATE SCHEMA IF NOT EXISTS banking.gold;
   ```

2. Upload the first delivery to staging (`data` container):

   | File | Folder |
   | --- | --- |
   | `customer_raw.csv` | `staging/customers/` |
   | `branch_raw.csv` | `staging/branches/` |
   | `product_raw.csv` | `staging/products/` |
   | `repayment_raw.csv` | `staging/repayments/` |

3. In every notebook, set `base = "abfss://data@<storage-account>.dfs.core.windows.net"`.
4. Run bronze, then the silver dims, then the silver fact, then gold. Or run the job (below).
5. To demo the MERGE, upload `branch_raw_2.csv` and `product_raw_2.csv` to their staging folders and rerun the branch and product bronze and silver notebooks.

## Deploy with Asset Bundles

Replace `<workspace-host>` in `databricks.yml` with your workspace URL (for example `https://adb-1234567890123456.7.azuredatabricks.net`, no trailing slash), then run:

```bash
databricks configure --token          # host + personal access token
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev banking_end_to_end_job
```

Notebook paths in `resources/job.yml` are relative (`../bronze/...`) and must use the same extension as the files in Git (`.py` here).

## CI/CD

| Workflow | Trigger | Action |
| --- | --- | --- |
| `deploy-dev.yml` | push to `dev` | `databricks bundle validate` + `deploy -t dev` |
| `deploy-prod.yml` | push to `main` | `databricks bundle validate` + `deploy -t prod` |

Repository secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`. Flow: `feature/*` → PR → `dev` → PR → `main`. Pushing files under `.github/workflows` needs a GitHub token with the Workflows permission.

## Expected results (sample data)

| Check | Value |
| --- | --- |
| Bronze rows (customer / branch / product / repayment) | 20 / 15 / 15 / 30 |
| `dim_branch` after the 2nd delivery | 16 rows |
| Total events / 30-day re-delinquencies | 30 / 8 (26.67%) |
| Re-delinquent events | EVT0002, 0005, 0006, 0008, 0010, 0015, 0021, 0025 |
| Highest-rate branch | BR004 Jeddah Tahlia, 66.67% (2 of 3) |
| Top product type | Credit Card, 4 of 8 |
| By segment | Retail 4, Affluent 3, Private 1 |

## Business questions (gold)

```sql
-- Highest re-delinquency branch
SELECT branch_name, region,
       ROUND(SUM(total_redelinquencies) * 100.0 / SUM(total_events), 2) AS rate_pct
FROM banking.gold.agg_redelinquency_branch_product
GROUP BY branch_name, region
ORDER BY rate_pct DESC;

-- Month-over-month trend
SELECT due_month, COUNT(*) AS events, SUM(redelinquency_30d_flag) AS redelinquencies
FROM banking.gold.fact_redelinquency_event
GROUP BY due_month
ORDER BY due_month;
```

## Consumption

- **Genie space** on `silver.fact_repayment`, `gold.agg_redelinquency_branch_product` and `gold.fact_redelinquency_event` for plain-English questions
- **AI/BI dashboard:** overall rate counter, rate by branch, re-delinquencies by product type and segment, monthly trend
- Both are shared with the `collections-analysts` group and opened from **Databricks One**

## Compliance notes

- **PDPL / SAMA:** silver keeps only the last 4 digits of the National ID / Iqama and the initials of names; bronze access should be limited to data engineers.
- **Dates:** stored in the Gregorian calendar; an optional Hijri column can be added for display.
- **Amounts:** in Saudi riyals (SAR).

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `Path must be absolute` | Use `abfss://data@<storage-account>.dfs.core.windows.net`, not `blob` and not without `abfss://` |
| A silver dim holds only the newest rows | Drop the table **and** delete its checkpoint folder together, then rerun |
| `cannot configure default credentials` | Replace the `<workspace-host>` placeholder in `databricks.yml`; check `databricks current-user me` |
| `notebook ... not found` in bundle validate | Add the file extension (`.py`) to every `notebook_path` |
| No metastore / `OPERATION_REQUIRES_UNITY_CATALOG` | Create or assign a Unity Catalog metastore to the workspace |

## Credits

The approach and step order follow [Azure-Databricks-Real-World-Healthcare-End-to-End-Project](https://github.com/anirvandecodes/Azure-Databricks-Real-World-Healthcare-End-to-End-Project), adapted from hospital readmissions to retail-banking re-delinquency.
