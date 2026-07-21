# hco-hcp-master-data-management
End-to-end Healthcare Master Data Management (MDM) pipeline built in Databricks for HCO and HCP data standardization, ID generation, validation, cross-reference creation, and golden record publishing.
# Healthcare Master Data Management (MDM) Pipeline

A Databricks-based Master Data Management (MDM) solution developed to standardize, validate, integrate, and publish Healthcare Organization (HCO) and Healthcare Professional (HCP) data from multiple source systems.

## Business Objective

Create a trusted source of truth for healthcare provider data by eliminating inconsistencies, validating records, generating unique identifiers, and publishing standardized datasets for downstream analytics.

## Workflow

### 1. Profiling

- Source data assessment
- Data quality analysis
- Attribute profiling
- Null and duplicate identification

### 2. Staging

- HCO ingestion
- HCP ingestion
- Address ingestion
- Identifier ingestion

### 3. Pre-MDM Processing

- Address standardization
- HCO validation
- HCP validation
- Data quality checks
- Business rule application

### 4. ID Generation

- Entity identifier creation
- Record linkage preparation
- Cross-reference generation

### 5. Publishing

- HCO Golden Records
- HCP Golden Records
- Cross-Reference Tables
- Master Identifier Tables

### 6. Testing

- Data validation
- Business rule validation
- Pipeline testing
