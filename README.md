# University Data Lakehouse

A Dagster-orchestrated data lakehouse for higher education, built on **Apache Iceberg + Trino + S3** with a **bronze → silver → gold** layering pattern. Designed for deployment on **Kubernetes (EKS)** with the AWS Glue Data Catalog.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DAGSTER ORCHESTRATION (EKS)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐   S3 Sensors    ┌──────────┐  Trino SQL  ┌────────────┐  │
│  │   BRONZE     │ ──────────────→ │  SILVER   │ ──────────→ │    GOLD     │  │
│  │  (Parquet)   │                 │ (Iceberg) │             │  (Iceberg)  │  │
│  └──────┬───────┘                 └─────┬─────┘             └──────┬──────┘  │
│         │                               │                          │         │
│  ┌──────┴───────┐                ┌──────┴──────┐           ┌──────┴──────┐  │
│  │ PeopleSoft   │                │ dim_students │           │ enrollment  │  │
│  │ SAP ERP      │                │ dim_courses  │           │   metrics   │  │
│  │ Salesforce   │                │ dim_employees│           │ financial   │  │
│  │ Higher-Ed    │                │ fact_enroll  │           │   aid       │  │
│  │  Systems     │                │ fact_finance │           │ admissions  │  │
│  └──────────────┘                │ fact_admits  │           │   funnel    │  │
│                                  └─────────────┘           └─────────────┘  │
├─────────────────────────────────────────────────────────────────────────────┤
│  S3 Bucket │ AWS Glue Data Catalog │ Trino Cluster │ OpenMetadata (opt.)    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Source Systems

| System | Type | Tables | Description |
|--------|------|--------|-------------|
| **PeopleSoft SIS** | Student Information | students, enrollments, courses, financial_aid | Core academic records |
| **SAP ERP** | Finance & HR | general_ledger, cost_centers, employees | Financial transactions and HR data |
| **Salesforce CRM** | Admissions & Alumni | contacts, opportunities, campaigns | Recruitment pipeline and alumni relations |
| **Higher-Ed Systems** | Operational | housing, athletics, conduct, placements | Campus life and field experience |

## Asset Graph

- **14 bronze assets** — raw Parquet files ingested from S3 landing zones
- **6 silver assets** — conformed dimension and fact Iceberg tables
- **3 gold assets** — analytics-ready aggregations for reporting
- **29 asset checks** — freshness, null PKs, duplicates, row counts, referential integrity
- **4 S3 sensors** — trigger downstream processing when raw data lands
- **4 scheduled jobs** — nightly ingestion, daily transforms, daily analytics, hourly admissions

## Getting Started

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) package manager

### Install & Run Locally (Demo Mode)

```bash
# Clone the repo
git clone https://github.com/thomasganka/university-data-lakehouse.git
cd university-data-lakehouse

# Install dependencies
uv sync

# Validate definitions
uv run dg check defs

# Start the Dagster UI
uv run dg dev
```

Open **http://localhost:3000** to explore the asset graph, sensors, schedules, and lineage.

Demo mode is enabled by default (`demo_mode: true` in YAML configs). Assets produce realistic metadata and row counts without requiring AWS or Trino infrastructure.

## End-to-End Implementation Guide

### Phase 1: Infrastructure Setup

#### 1.1 — S3 Bucket & Landing Zones

```bash
aws s3 mb s3://university-data-lakehouse --region us-east-1

# Create landing zone prefixes for each source system
for prefix in landing/peoplesoft landing/sap landing/salesforce landing/highered; do
  aws s3api put-object --bucket university-data-lakehouse --key "${prefix}/"
done
```

#### 1.2 — AWS Glue Data Catalog

```bash
# Create databases for each layer
aws glue create-database --database-input '{"Name": "university_bronze"}'
aws glue create-database --database-input '{"Name": "university_silver"}'
aws glue create-database --database-input '{"Name": "university_gold"}'
```

#### 1.3 — Trino Cluster on EKS

Deploy Trino with the Iceberg connector pointing to Glue:

```yaml
# trino-catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=glue
hive.metastore.glue.region=us-east-1
iceberg.file-format=PARQUET
hive.s3.endpoint=s3.us-east-1.amazonaws.com
```

Verify connectivity:

```sql
SHOW SCHEMAS FROM iceberg;
-- Should list: university_bronze, university_silver, university_gold
```

#### 1.4 — Dagster on EKS

Deploy using the [Dagster Helm chart](https://docs.dagster.io/deployment/guides/kubernetes/deploying-with-helm):

```bash
helm repo add dagster https://dagster-io.github.io/helm
helm install dagster dagster/dagster \
  --namespace dagster \
  --set dagsterWebserver.replicaCount=1 \
  --set dagsterDaemon.replicaCount=1
```

### Phase 2: Connect Source Systems

#### 2.1 — PeopleSoft SIS → S3

Set up an extract job (e.g., via PeopleSoft Integration Broker or a scheduled SQL extract) that writes Parquet files to:

```
s3://university-data-lakehouse/landing/peoplesoft/students/
s3://university-data-lakehouse/landing/peoplesoft/enrollments/
s3://university-data-lakehouse/landing/peoplesoft/courses/
s3://university-data-lakehouse/landing/peoplesoft/financial_aid/
```

#### 2.2 — SAP ERP → S3

Use SAP Data Intelligence or a CDC tool (Fivetran, Airbyte, AWS DMS) to replicate:

```
s3://university-data-lakehouse/landing/sap/general_ledger/
s3://university-data-lakehouse/landing/sap/cost_centers/
s3://university-data-lakehouse/landing/sap/employees/
```

#### 2.3 — Salesforce CRM → S3

Use Salesforce Data Cloud, Fivetran, or Airbyte to sync:

```
s3://university-data-lakehouse/landing/salesforce/contacts/
s3://university-data-lakehouse/landing/salesforce/opportunities/
s3://university-data-lakehouse/landing/salesforce/campaigns/
```

#### 2.4 — Higher-Ed Operational Systems → S3

Export from campus systems (StarRez, ARMS, Maxient, etc.):

```
s3://university-data-lakehouse/landing/highered/housing_assignments/
s3://university-data-lakehouse/landing/highered/athletics_rosters/
s3://university-data-lakehouse/landing/highered/student_conduct/
s3://university-data-lakehouse/landing/highered/field_placements/
```

### Phase 3: Switch to Production Mode

Edit the YAML config files in `src/university_data_lakehouse/defs/` to disable demo mode and configure real connections:

**Bronze** (`defs/bronze_ingestion/defs.yaml`):

```yaml
attributes:
  demo_mode: false          # ← Disable demo mode
  s3_bucket: university-data-lakehouse
  aws_region: us-east-1
```

**Silver** (`defs/silver_transforms/defs.yaml`):

```yaml
attributes:
  demo_mode: false          # ← Disable demo mode
  trino_host: trino.university-lakehouse.internal
  trino_port: 8443
  trino_catalog: iceberg
  trino_schema: silver
```

**Gold** (`defs/gold_analytics/defs.yaml`):

```yaml
attributes:
  demo_mode: false          # ← Disable demo mode
  trino_host: trino.university-lakehouse.internal
  trino_port: 8443
  trino_catalog: iceberg
  trino_schema: gold
  openmetadata_host: "http://openmetadata.internal:8585"  # ← Optional
```

Install production dependencies:

```bash
uv add boto3 trino
```

### Phase 4: Enable Sensors & Schedules

In the Dagster UI or via YAML, activate the S3 sensors and schedules:

| Automation | Schedule | Description |
|------------|----------|-------------|
| `bronze_nightly_ingestion` | `0 2 * * *` | Full ingestion at 2am ET |
| `silver_daily_transforms` | `0 4 * * *` | Trino transforms at 4am ET |
| `gold_daily_analytics` | `0 6 * * *` | Analytics refresh at 6am ET |
| `admissions_hourly_refresh` | `0 * * * *` | Hourly admissions pipeline (enable manually) |
| `s3_sensor_peoplesoft` | event-driven | Triggers on new PeopleSoft files |
| `s3_sensor_sap` | event-driven | Triggers on new SAP files |
| `s3_sensor_salesforce` | event-driven | Triggers on new Salesforce files |
| `s3_sensor_highered` | event-driven | Triggers on new Higher-Ed files |

### Phase 5: Data Quality & Lineage

#### Asset Checks (built-in)

Every layer includes automated quality checks:

- **Bronze**: Freshness checks — verify data arrived within SLA window
- **Silver**: `not_null_pk`, `no_duplicates`, `row_count`, `referential_integrity`, `value_range`
- **Gold**: Completeness checks — row counts and null key column validation

View check results in the Dagster UI under each asset's **Checks** tab.

#### Column-Level Lineage

Column lineage is embedded in asset metadata and visible in the Dagster UI. Example flow:

```
PeopleSoft.students.first_name  ─┐
                                  ├→  silver.dim_students.full_name  →  gold.enrollment_metrics.avg_gpa
PeopleSoft.students.last_name   ─┘                                       (via student join)
```

#### OpenMetadata Integration (Optional)

Set `openmetadata_host` in the gold analytics config to enable automatic lineage registration:

```yaml
openmetadata_host: "http://openmetadata.internal:8585"
```

This will push table-level lineage to OpenMetadata via its REST API after each gold model materialization.

## Project Structure

```
university-data-lakehouse/
├── src/university_data_lakehouse/
│   ├── components/                         # Component class definitions
│   │   ├── s3_data_landing_component.py    # Bronze: S3 ingestion + sensors
│   │   ├── trino_iceberg_transform_component.py  # Silver: Trino/Iceberg transforms
│   │   ├── gold_analytics_component.py     # Gold: analytics models
│   │   └── scheduled_job_component.py      # Scheduling with asset selection
│   ├── defs/                               # Component instance YAML configs
│   │   ├── bronze_ingestion/defs.yaml
│   │   ├── silver_transforms/defs.yaml
│   │   ├── gold_analytics/defs.yaml
│   │   └── schedules/defs.yaml
│   └── definitions.py                      # Dagster entry point
├── pyproject.toml
└── uv.lock
```

## Useful Commands

```bash
uv run dg check defs              # Validate all definitions
uv run dg list defs               # List all assets, sensors, schedules
uv run dg list defs --json        # JSON output for scripting
uv run dg dev                     # Start local dev server
```

## Learn More

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster Components Guide](https://docs.dagster.io/guides/build/projects/moving-to-components)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Trino Documentation](https://trino.io/docs/current/)
- [OpenMetadata](https://open-metadata.org/)
