# End-to-End-Retail-Lakehouse-with-CDC-CI-CD-DABs-

## 🎯 Overview

A production-grade, cloud-native Data Lakehouse pipeline built on Azure Databricks. This project demonstrates advanced data engineering patterns including Change Data Capture (CDC) using `apply_changes`, Delta Live Tables (DLT), Unity Catalog governance, and fully automated CI/CD using Databricks Asset Bundles (DABs) and GitHub Actions.

## 🏗️ Architecture

```
ADLS Gen2 (raw)                 DLT Pipeline (Unity Catalog)
──────────────                  ─────────────────────────────
customers/*.csv     ──►  Bronze: bronze_customers
                                  (Auto Loader, batch/CSV)

orders_cdc/*.json   ──►  Bronze: bronze_orders_cdc_raw
                                  (Auto Loader, streaming/JSON)
                                       │
                                       ▼
                          Silver: silver_orders
                                  (apply_changes → SCD Type 1)
                                       │
                                       ▼
                          Gold:   gold_daily_sales
                                  (BI-ready daily aggregates)
```

* **Ingestion (Bronze):** Auto Loader ingests batch CSVs and streaming CDC JSON (Inserts/Updates/Deletes) from ADLS Gen2.
* **Refinement (Silver):** DLT processes the raw CDC stream, applying `apply_changes()` to seamlessly merge state changes from an OLTP source into a Type 1 SCD table.
* **Aggregation (Gold):** Business-level aggregations optimized for BI reporting.
* **Deployment:** Code is pushed to GitHub, which triggers GitHub Actions to run `databricks bundle deploy`, updating the DLT pipeline in Dev/Prod without manual UI clicks.

## 📁 Project Structure

```
retail-lakehouse-cdc-dab/
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD: deploys the bundle on push
├── src/
│   ├── bronze_customers.py     # Auto Loader batch ingestion (CSV)
│   ├── silver_orders_cdc.py    # Streaming CDC + apply_changes (SCD Type 1)
│   └── gold_daily_sales.py     # BI-ready daily aggregation
├── databricks.yml              # DAB config: pipeline, job, dev/prod targets
├── resources/
│   └── sample_data/
│       ├── orders_cdc_sample.json   # Sample CDC events (insert/update/delete)
│       └── customers_sample.csv     # Sample customer dimension data
└── README.md
```

## ⚙️ Tech Stack

* **Orchestration/Deploy:** Databricks Asset Bundles (DABs), GitHub Actions CI/CD
* **Processing:** Delta Live Tables (DLT), PySpark, Structured Streaming, Auto Loader
* **Storage:** ADLS Gen2, Delta Lake
* **Governance:** Unity Catalog

## 🚀 How to Run

1. Clone the repo:
   ```bash
   git clone <your-repo-link>
   cd retail-lakehouse-cdc-dab
   ```
2. Authenticate the Databricks CLI:
   ```bash
   databricks configure
   ```
3. Update `databricks.yml` and the `abfss://` paths in `src/*.py` with your workspace ID, catalog name, and storage account.
4. Deploy to dev:
   ```bash
   databricks bundle deploy --target dev
   ```
5. Run the pipeline:
   ```bash
   databricks bundle run retail_lakehouse_job --target dev
   ```
6. Deploy to prod (via CI/CD): merge to `main` — GitHub Actions handles the rest.

## 🔑 CDC Logic (the core of this project)

`src/silver_orders_cdc.py` uses DLT's `apply_changes` API to merge a stream of raw CDC events (`INSERT` / `UPDATE` / `DELETE`) into a clean, current-state `silver_orders` table:

* `keys=["order_id"]` — the natural key changes are merged on
* `sequence_by` — resolves out-of-order/late-arriving events correctly
* `apply_as_deletes` — honors upstream deletes instead of leaving orphaned rows
* `stored_as_scd_type=1` — keeps only the latest state (no history retained)

The included `orders_cdc_sample.json` walks one order through insert → shipped → delivered, and another through insert → delete, so you can see `apply_changes` collapse the stream correctly.

## 🚨 Setup Steps Before This Runs End-to-End

1. **Update placeholders:** Replace `<your_workspace_id>`, `<your_catalog_name>`, and `<your_storage_account>` in `databricks.yml` and `src/*.py` with your real Azure Databricks values.
2. **GitHub Secrets:** In your repo → *Settings → Secrets and variables → Actions*, add:
   * `DATABRICKS_HOST`: `https://<your-workspace-id>.azuredatabricks.net`
   * `DATABRICKS_TOKEN`: a Databricks Personal Access Token with workspace + DAB permissions
3. **Land the sample data:** Upload `resources/sample_data/customers_sample.csv` and `orders_cdc_sample.json` to the ADLS Gen2 paths referenced in `databricks.yml` (or point the paths at your own landing zone) before running the pipeline.

## 📜 License

MIT
