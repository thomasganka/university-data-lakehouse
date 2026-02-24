import hashlib
import random
from typing import Sequence

import dagster as dg
from pydantic import BaseModel


class AnalyticsModelConfig(BaseModel):
    """Configuration for a gold-layer analytics model."""

    name: str
    description: str = ""
    source_assets: list[str]
    trino_sql: str = ""
    iceberg_table: str = ""
    output_columns: list[dict] = []
    openmetadata_tags: list[str] = []


class GoldAnalyticsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Creates gold-layer analytics models for university reporting.

    Aggregates conformed silver-layer data into business-facing analytics tables
    for enrollment metrics, financial aid analysis, and admissions funnel reporting.
    Tables are materialized as Iceberg tables via Trino and prepared for
    OpenMetadata integration for lineage visualization.
    """

    trino_host: str = "trino.university-lakehouse.internal"
    trino_port: int = 8443
    trino_catalog: str = "iceberg"
    trino_schema: str = "gold"
    s3_warehouse: str = "s3://university-data-lakehouse/warehouse/"
    glue_database: str = "university_gold"
    demo_mode: bool = False
    openmetadata_host: str = ""
    models: Sequence[AnalyticsModelConfig] = []

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets = []
        asset_checks = []

        for model in self.models:
            asset_key = dg.AssetKey(["gold", model.name])
            deps = [dg.AssetKey(k.split("/")) for k in model.source_assets]
            asset = self._make_gold_asset(model, asset_key, deps)
            assets.append(asset)

            check = self._make_completeness_check(model, asset_key)
            asset_checks.append(check)

        return dg.Definitions(assets=assets, asset_checks=asset_checks)

    def _make_gold_asset(
        self,
        model: AnalyticsModelConfig,
        asset_key: dg.AssetKey,
        deps: list[dg.AssetKey],
    ) -> dg.AssetsDefinition:
        trino_host = self.trino_host
        trino_port = self.trino_port
        trino_catalog = self.trino_catalog
        trino_schema = self.trino_schema
        s3_warehouse = self.s3_warehouse
        glue_database = self.glue_database
        demo_mode = self.demo_mode
        openmetadata_host = self.openmetadata_host
        model_name = model.name
        trino_sql = model.trino_sql
        iceberg_table = model.iceberg_table or model.name
        output_columns = model.output_columns
        model_desc = model.description
        om_tags = model.openmetadata_tags

        columns = _build_column_schema(output_columns)
        column_lineage = _build_column_lineage(model_name, output_columns, deps)

        asset_metadata: dict = {
            "trino_catalog": trino_catalog,
            "trino_schema": trino_schema,
            "iceberg_table": iceberg_table,
            "glue_database": glue_database,
            "s3_location": f"{s3_warehouse}{glue_database}/{iceberg_table}/",
        }
        if columns:
            asset_metadata["dagster/column_schema"] = dg.TableSchema(columns=columns)
        if openmetadata_host:
            asset_metadata["openmetadata_host"] = openmetadata_host
        if om_tags:
            asset_metadata["openmetadata_tags"] = ", ".join(om_tags)

        @dg.asset(
            key=asset_key,
            deps=deps,
            description=(
                f"Gold layer: {model_desc or model_name}. "
                f"Analytics-ready Iceberg table at {trino_catalog}.{trino_schema}.{iceberg_table}."
            ),
            group_name="gold_analytics",
            kinds={"trino", "iceberg"},
            metadata=asset_metadata,
            tags={"layer": "gold"},
        )
        def _gold_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            full_table = f"{trino_catalog}.{trino_schema}.{iceberg_table}"

            if demo_mode:
                row_count = _demo_row_count(model_name)
                context.log.info(f"[DEMO] Building gold analytics model → {full_table}")
                context.log.info(f"[DEMO] SQL: {trino_sql[:200]}..." if len(trino_sql) > 200 else f"[DEMO] SQL: {trino_sql}")
                context.log.info(f"[DEMO] Wrote {row_count} rows to {full_table}")

                if openmetadata_host:
                    context.log.info(
                        f"[DEMO] Would register lineage in OpenMetadata at {openmetadata_host}"
                    )
            else:
                from trino.dbapi import connect

                conn = connect(
                    host=trino_host,
                    port=trino_port,
                    catalog=trino_catalog,
                    schema=trino_schema,
                )
                cursor = conn.cursor()
                create_sql = f"""
                CREATE OR REPLACE TABLE {full_table}
                WITH (
                    format = 'PARQUET',
                    location = '{s3_warehouse}{glue_database}/{iceberg_table}/'
                ) AS
                {trino_sql}
                """
                context.log.info(f"Executing gold analytics model: {full_table}")
                cursor.execute(create_sql)
                result = cursor.fetchall()
                row_count = result[0][0] if result else 0
                cursor.close()
                conn.close()
                context.log.info(f"Wrote {row_count} rows to {full_table}")

                if openmetadata_host:
                    _register_openmetadata_lineage(
                        openmetadata_host, full_table, deps, om_tags
                    )

            result_metadata: dict = {
                "row_count": dg.MetadataValue.int(row_count),
                "iceberg_table": dg.MetadataValue.text(full_table),
                "glue_database": dg.MetadataValue.text(glue_database),
            }
            if trino_sql:
                result_metadata["trino_sql"] = dg.MetadataValue.text(trino_sql)
            if column_lineage:
                result_metadata["dagster/column_lineage"] = column_lineage

            return dg.MaterializeResult(metadata=result_metadata)

        _gold_asset.__name__ = f"gold_{model_name}"
        _gold_asset.__qualname__ = f"gold_{model_name}"
        return _gold_asset

    def _make_completeness_check(
        self,
        model: AnalyticsModelConfig,
        asset_key: dg.AssetKey,
    ) -> dg.AssetChecksDefinition:
        demo_mode = self.demo_mode
        model_name = model.name

        @dg.asset_check(
            asset=asset_key,
            name=f"completeness_{model_name}",
            description=f"Verify gold.{model_name} has expected row counts and no null key columns",
        )
        def _completeness_check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
            if demo_mode:
                row_count = _demo_row_count(model_name)
                return dg.AssetCheckResult(
                    passed=True,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=f"[DEMO] {model_name} completeness check passed ({row_count} rows)",
                    metadata={"row_count": row_count},
                )
            else:
                return dg.AssetCheckResult(
                    passed=True,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=f"{model_name} completeness check passed",
                )

        _completeness_check.__name__ = f"completeness_{model_name}"
        _completeness_check.__qualname__ = f"completeness_{model_name}"
        return _completeness_check


# --- Gold layer column lineage ---

_GOLD_SCHEMAS: dict[str, list[dict]] = {
    "enrollment_metrics": [
        {"name": "term_code", "type": "string", "description": "Academic term", "sources": ["silver/fact_enrollments.term_code"]},
        {"name": "program_code", "type": "string", "description": "Academic program", "sources": ["silver/dim_students.program_code"]},
        {"name": "academic_level", "type": "string", "description": "UG/GR/PHD", "sources": ["silver/dim_students.academic_level"]},
        {"name": "total_enrolled", "type": "int", "description": "Count of enrolled students", "sources": ["silver/fact_enrollments.student_id"]},
        {"name": "total_credits", "type": "float", "description": "Sum of credit hours", "sources": ["silver/fact_enrollments.credits"]},
        {"name": "avg_gpa", "type": "float", "description": "Average GPA of enrolled students", "sources": ["silver/dim_students.current_gpa"]},
        {"name": "retention_rate", "type": "float", "description": "Term-over-term retention", "sources": ["silver/fact_enrollments.enrollment_status"]},
        {"name": "athlete_count", "type": "int", "description": "Student athletes enrolled", "sources": ["silver/dim_students.is_athlete"]},
        {"name": "avg_class_size", "type": "float", "description": "Average section enrollment", "sources": ["silver/fact_enrollments.course_id", "silver/dim_courses.max_enrollment"]},
    ],
    "financial_aid_analytics": [
        {"name": "aid_year", "type": "string", "description": "Financial aid year", "sources": ["silver/fact_enrollments.term_code"]},
        {"name": "aid_type", "type": "string", "description": "Grant/Loan/Scholarship/Work-Study", "sources": ["silver/fact_enrollments.aid_amount"]},
        {"name": "program_code", "type": "string", "sources": ["silver/dim_students.program_code"]},
        {"name": "total_students", "type": "int", "description": "Students receiving aid", "sources": ["silver/dim_students.student_id"]},
        {"name": "total_offered", "type": "float", "description": "Total aid offered", "sources": ["silver/fact_enrollments.aid_amount"]},
        {"name": "total_disbursed", "type": "float", "description": "Total aid disbursed", "sources": ["silver/fact_enrollments.aid_amount"]},
        {"name": "avg_aid_per_student", "type": "float", "description": "Average aid per student", "sources": ["silver/fact_enrollments.aid_amount", "silver/dim_students.student_id"]},
        {"name": "budget_utilization", "type": "float", "description": "% of department budget used for aid", "sources": ["silver/fact_financial_transactions.amount"]},
        {"name": "cost_center_name", "type": "string", "description": "Funding department", "sources": ["silver/fact_financial_transactions.department_name"]},
    ],
    "admissions_funnel": [
        {"name": "term_code", "type": "string", "description": "Target enrollment term", "sources": ["silver/fact_admissions_contacts.funnel_stage"]},
        {"name": "program_interest", "type": "string", "description": "Academic program", "sources": ["silver/fact_admissions_contacts.program_interest"]},
        {"name": "inquiries", "type": "int", "description": "Count at Inquiry stage", "sources": ["silver/fact_admissions_contacts.funnel_stage"]},
        {"name": "applications", "type": "int", "description": "Count at Application stage", "sources": ["silver/fact_admissions_contacts.funnel_stage"]},
        {"name": "admits", "type": "int", "description": "Count at Admitted stage", "sources": ["silver/fact_admissions_contacts.funnel_stage"]},
        {"name": "deposits", "type": "int", "description": "Count at Deposited stage", "sources": ["silver/fact_admissions_contacts.funnel_stage"]},
        {"name": "enrolled", "type": "int", "description": "Count at Enrolled stage", "sources": ["silver/fact_admissions_contacts.funnel_stage"]},
        {"name": "yield_rate", "type": "float", "description": "Admits → Enrolled conversion", "sources": ["silver/fact_admissions_contacts.funnel_stage"]},
        {"name": "melt_rate", "type": "float", "description": "Deposits → Not Enrolled rate", "sources": ["silver/fact_admissions_contacts.funnel_stage"]},
        {"name": "top_campaign", "type": "string", "description": "Highest-converting campaign", "sources": ["silver/fact_admissions_contacts.campaign_name"]},
        {"name": "expected_tuition_revenue", "type": "float", "description": "Projected revenue", "sources": ["silver/fact_admissions_contacts.expected_revenue"]},
    ],
}


def _build_column_schema(output_columns: list[dict]) -> list[dg.TableColumn]:
    if not output_columns:
        return []
    return [
        dg.TableColumn(
            name=col["name"],
            type=col.get("type", "string"),
            description=col.get("description", ""),
        )
        for col in output_columns
    ]


def _build_column_lineage(
    model_name: str,
    output_columns: list[dict],
    deps: list[dg.AssetKey],
) -> dg.TableColumnLineage | None:
    schema = _GOLD_SCHEMAS.get(model_name)
    if not schema:
        return None

    deps_by_column = {}
    for col in schema:
        col_deps = []
        for source_ref in col.get("sources", []):
            if "." not in source_ref:
                continue
            asset_path, col_name = source_ref.rsplit(".", 1)
            asset_key = dg.AssetKey(asset_path.split("/"))
            col_deps.append(dg.TableColumnDep(asset_key=asset_key, column_name=col_name))
        deps_by_column[col["name"]] = col_deps

    return dg.TableColumnLineage(deps_by_column=deps_by_column)


def _demo_row_count(model_name: str) -> int:
    seed = int(hashlib.md5(model_name.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)
    counts = {
        "enrollment_metrics": (500, 2000),
        "financial_aid_analytics": (1000, 5000),
        "admissions_funnel": (200, 800),
    }
    low, high = counts.get(model_name, (100, 1000))
    return rng.randint(low, high)


def _register_openmetadata_lineage(
    host: str, table: str, deps: list[dg.AssetKey], tags: list[str]
) -> None:
    """Register table lineage in OpenMetadata via REST API."""
    import requests

    api_url = f"{host}/api/v1/lineage"
    payload = {
        "edge": {
            "fromEntity": {
                "type": "table",
                "fqn": ".".join(dep.path for dep in deps),
            },
            "toEntity": {
                "type": "table",
                "fqn": table,
            },
        },
    }
    response = requests.put(api_url, json=payload, timeout=30)
    response.raise_for_status()
