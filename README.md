# Singapore Vessel Traffic Data Engineering Pipeline

An end-to-end data engineering project that processes historical Singapore vessel arrival data into analytics-ready datasets using Databricks, PySpark, Delta Lake, and Apache Airflow.

## Project Overview

This project explores monthly vessel arrivals in Singapore, including vessel types, arrival counts, and gross tonnage. It demonstrates a data pipeline that transforms raw data into structured datasets for analysis and visualisation.

The workflow combines data transformation in Databricks with orchestration using Apache Airflow, with Power BI used to present the results.

## Architecture

**CSV → Bronze → Silver → Gold → Power BI**

- **Source:** Historical monthly vessel arrival data in CSV format.
- **Bronze:** Ingest the source data into a Delta table.
- **Silver:** Clean and validate records, handle missing values, filter invalid counts and tonnage, and remove duplicates.
- **Gold:** Produce aggregated datasets for monthly vessel traffic, vessel-type summaries, and traffic trends.
- **Visualisation:** Build a Power BI dashboard to explore vessel arrivals and gross tonnage.

## Tech Stack

- **Python**
- **PySpark**
- **Databricks**
- **Delta Lake**
- **Apache Airflow**
- **Docker Compose**
- **Power BI**

## Orchestration

Apache Airflow orchestrates the Databricks pipeline through a Databricks job API integration. The DAG triggers the job and monitors its execution status.

The local Airflow environment is run using Docker Compose, while credentials are configured through an Airflow connection rather than hardcoded in the DAG.

## Data Outputs

The pipeline produces three primary Gold datasets:

| Table | Purpose |
|---|---|
| `gold_monthly_vessel_traffic` | Monthly vessel arrival totals and gross tonnage |
| `gold_vessel_traffic_trends` | Monthly traffic trends and period-over-period changes |
| `gold_vessel_type_summary` | Aggregated vessel arrivals and gross tonnage by vessel type |

## Dashboard

The Power BI dashboard presents:

- Total vessel arrivals
- Total gross tonnage
- Number of vessel types
- Monthly vessel arrival trends
- Vessel arrivals by type
- Gross tonnage by vessel type

## Key Learning Areas

- Implementing a Bronze–Silver–Gold data pipeline
- Transforming data with PySpark and Delta tables
- Applying basic data quality checks
- Aggregating data for analytical use cases
- Orchestrating Databricks jobs with Apache Airflow
- Connecting data engineering outputs to Power BI

## Future Improvements

- Implement incremental ingestion and safe reruns
- Expand automated data quality tests
- Improve pipeline monitoring and failure handling
- Add more analytical metrics and dashboard interactions
