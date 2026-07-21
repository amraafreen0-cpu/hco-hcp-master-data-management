# Healthcare Master Data Management (MDM) Pipeline

A Databricks-based Master Data Management (MDM) solution built to standardize, validate, integrate, and publish Healthcare Organization (HCO) and Healthcare Professional (HCP) data using Medallion Architecture.

---

## Project Objective

Develop an end-to-end MDM framework that ingests data from multiple sources, applies data quality and standardization rules, generates unique identifiers, and publishes trusted master datasets for downstream analytics and reporting.

---

## Architecture

This solution follows the Databricks Medallion Architecture approach.

### Bronze Layer (01_Staging)

Raw source data is ingested into staging tables.

Entities processed:

- HCO
- HCP
- Address
- Identifiers

### Silver Layer (02_PreMDM)

Business rules and data quality frameworks are applied.

Activities include:

- Data standardization
- Address cleansing
- Data quality validation
- Identifier generation
- Data enrichment
- Record validation

### Gold Layer (03_Publish)

Curated master datasets are published for downstream consumption.

Outputs include:

- HCO Master
- HCP Master
- Cross Reference Tables
- Identifier Tables

---

## Project Workflow

```text
Profiling
    ↓
Bronze Layer (Staging)
    ↓
Silver Layer (Pre-MDM)
    ↓
Gold Layer (Publish)
    ↓
Testing & Validation
```

---

## Workflow Orchestration

The pipeline is orchestrated using Databricks Workflows.

Processing Sequence:

```text
STG_ADDR
STG_HCO
STG_HCP
STG_ID
     ↓
ID_GEN
     ↓
PREMDM_ADDR
PREMDM_HCO
PREMDM_HCP
PREMDM_ID
     ↓
PUB_HCP_XREF
PUB_HCO_XREF
     ↓
PUB_HCO
PUB_HCP
PUB_ID
```

---

## Key Capabilities

- Data Profiling
- Data Standardization
- Data Quality Management (DQM)
- Entity Validation
- Identifier Generation
- Cross Reference Management
- Master Record Publishing
- Workflow Automation

---

## Technologies Used

- Databricks
- PySpark
- SQL
- Azure
- Delta Lake
- Databricks Workflows

---

## Repository Structure

```text
00_Profiling        → Data Assessment
01_Staging          → Bronze Layer
02_PreMDM           → Silver Layer
03_Publish          → Gold Layer
04_Testing          → Validation & QA
```

---

## Business Value

The solution creates a trusted source of healthcare provider data by reducing duplication, improving data quality, enabling consistent reporting, and supporting downstream analytics use cases.

---

## Disclaimer

This project uses sample/demo healthcare data for demonstration and portfolio purposes only. No confidential client or patient information is included.
