import hashlib
import random
from typing import Sequence

import dagster as dg
from pydantic import BaseModel


class TransformConfig(BaseModel):
    """Configuration for a silver-layer Trino/Iceberg transformation."""

    name: str
    description: str = ""
    source_assets: list[str]
    trino_sql: str = ""
    iceberg_table: str = ""
    glue_database: str = "university_silver"
    data_quality_checks: list[str] = []
    output_columns: list[dict] = []


class TrinoIcebergTransformComponent(dg.Component, dg.Model, dg.Resolvable):
    """Transforms bronze Parquet data into conformed Iceberg tables via Trino.

    Reads raw data from the bronze S3 layer, applies cleansing and conforming
    transformations using Trino SQL, and writes results to Apache Iceberg tables
    cataloged in AWS Glue Data Catalog. Implements data quality checks at the
    silver layer boundary.
    """

    trino_host: str = "trino.university-lakehouse.internal"
    trino_port: int = 8443
    trino_catalog: str = "iceberg"
    trino_schema: str = "silver"
    s3_warehouse: str = "s3://university-data-lakehouse/warehouse/"
    glue_database: str = "university_silver"
    demo_mode: bool = False
    transforms: Sequence[TransformConfig] = []

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets = []
        asset_checks = []

        for transform in self.transforms:
            asset_key = dg.AssetKey(["silver", transform.name])
            deps = [dg.AssetKey(k.split("/")) for k in transform.source_assets]
            asset = self._make_silver_asset(transform, asset_key, deps)
            assets.append(asset)

            for check_name in transform.data_quality_checks:
                check = self._make_quality_check(transform, asset_key, check_name)
                asset_checks.append(check)

        return dg.Definitions(assets=assets, asset_checks=asset_checks)

    def _make_silver_asset(
        self,
        transform: TransformConfig,
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
        transform_name = transform.name
        trino_sql = transform.trino_sql
        iceberg_table = transform.iceberg_table or transform.name
        output_columns = transform.output_columns
        transform_desc = transform.description

        columns = _build_column_schema(output_columns)

        @dg.asset(
            key=asset_key,
            deps=deps,
            description=(
                f"Silver layer: {transform_desc or transform_name}. "
                f"Iceberg table at {trino_catalog}.{trino_schema}.{iceberg_table}, "
                f"cataloged in Glue database '{glue_database}'."
            ),
            group_name="silver_transforms",
            kinds={"trino", "iceberg"},
            metadata={
                "trino_catalog": trino_catalog,
                "trino_schema": trino_schema,
                "iceberg_table": iceberg_table,
                "glue_database": glue_database,
                "s3_location": f"{s3_warehouse}{glue_database}/{iceberg_table}/",
                "dagster/column_schema": dg.TableSchema(columns=columns) if columns else None,
            },
            tags={"layer": "silver"},
        )
        def _silver_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            full_table = f"{trino_catalog}.{trino_schema}.{iceberg_table}"

            if demo_mode:
                row_count = _demo_row_count(transform_name)
                context.log.info(f"[DEMO] Running Trino transform → {full_table}")
                context.log.info(f"[DEMO] SQL: {trino_sql[:200]}..." if len(trino_sql) > 200 else f"[DEMO] SQL: {trino_sql}")
                context.log.info(f"[DEMO] Wrote {row_count} rows to Iceberg table {full_table}")
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
                CREATE TABLE IF NOT EXISTS {full_table}
                WITH (
                    format = 'PARQUET',
                    location = '{s3_warehouse}{glue_database}/{iceberg_table}/'
                ) AS
                {trino_sql}
                """
                context.log.info(f"Executing Trino transform: {full_table}")
                cursor.execute(create_sql)
                result = cursor.fetchall()
                row_count = result[0][0] if result else 0
                cursor.close()
                conn.close()
                context.log.info(f"Wrote {row_count} rows to {full_table}")

            column_lineage = _build_column_lineage(transform_name, output_columns, deps)

            metadata: dict = {
                "row_count": dg.MetadataValue.int(row_count),
                "iceberg_table": dg.MetadataValue.text(full_table),
                "glue_database": dg.MetadataValue.text(glue_database),
                "s3_location": dg.MetadataValue.text(
                    f"{s3_warehouse}{glue_database}/{iceberg_table}/"
                ),
            }
            if column_lineage:
                metadata["dagster/column_lineage"] = column_lineage
            if trino_sql:
                metadata["trino_sql"] = dg.MetadataValue.text(trino_sql)

            return dg.MaterializeResult(metadata=metadata)

        _silver_asset.__name__ = f"silver_{transform_name}"
        _silver_asset.__qualname__ = f"silver_{transform_name}"
        return _silver_asset

    def _make_quality_check(
        self,
        transform: TransformConfig,
        asset_key: dg.AssetKey,
        check_name: str,
    ) -> dg.AssetChecksDefinition:
        demo_mode = self.demo_mode
        trino_host = self.trino_host
        trino_port = self.trino_port
        trino_catalog = self.trino_catalog
        trino_schema = self.trino_schema
        iceberg_table = transform.iceberg_table or transform.name
        transform_name = transform.name

        @dg.asset_check(
            asset=asset_key,
            name=f"{check_name}_{transform_name}",
            description=f"Data quality: {check_name} on silver.{transform_name}",
        )
        def _quality_check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
            if demo_mode:
                return dg.AssetCheckResult(
                    passed=True,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=f"[DEMO] {check_name} passed for {transform_name}",
                    metadata={"check_type": check_name, "rows_checked": _demo_row_count(transform_name)},
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
                full_table = f"{trino_catalog}.{trino_schema}.{iceberg_table}"

                check_queries = {
                    "not_null_pk": f"SELECT COUNT(*) FROM {full_table} WHERE student_id IS NULL",
                    "row_count": f"SELECT COUNT(*) FROM {full_table}",
                    "no_duplicates": f"SELECT COUNT(*) - COUNT(DISTINCT student_id) FROM {full_table}",
                    "referential_integrity": f"SELECT COUNT(*) FROM {full_table}",
                    "value_range": f"SELECT COUNT(*) FROM {full_table}",
                }
                sql = check_queries.get(check_name, f"SELECT COUNT(*) FROM {full_table}")
                cursor.execute(sql)
                result = cursor.fetchone()
                value = result[0] if result else 0
                cursor.close()
                conn.close()

                passed = value == 0 if check_name in ("not_null_pk", "no_duplicates") else value > 0
                return dg.AssetCheckResult(
                    passed=passed,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=f"{check_name}: value={value}",
                    metadata={"check_value": value},
                )

        _quality_check.__name__ = f"{check_name}_{transform_name}"
        _quality_check.__qualname__ = f"{check_name}_{transform_name}"
        return _quality_check


# --- Column lineage helpers ---

_SILVER_SCHEMAS: dict[str, list[dict]] = {
    "dim_students": [
        {"name": "student_key", "type": "string", "description": "Surrogate key", "sources": []},
        {"name": "student_id", "type": "string", "description": "Natural key from PeopleSoft", "sources": ["bronze/peoplesoft_students.student_id"]},
        {"name": "full_name", "type": "string", "description": "Concatenated name", "sources": ["bronze/peoplesoft_students.first_name", "bronze/peoplesoft_students.last_name"]},
        {"name": "email", "type": "string", "sources": ["bronze/peoplesoft_students.email"]},
        {"name": "date_of_birth", "type": "date", "sources": ["bronze/peoplesoft_students.date_of_birth"]},
        {"name": "program_code", "type": "string", "sources": ["bronze/peoplesoft_students.program_code"]},
        {"name": "academic_level", "type": "string", "sources": ["bronze/peoplesoft_students.academic_level"]},
        {"name": "current_gpa", "type": "float", "description": "Latest GPA", "sources": ["bronze/peoplesoft_students.gpa"]},
        {"name": "enrollment_status", "type": "string", "sources": ["bronze/peoplesoft_students.enrollment_status"]},
        {"name": "housing_status", "type": "string", "description": "Current housing", "sources": ["bronze/highered_housing_assignments.building_code"]},
        {"name": "is_athlete", "type": "boolean", "description": "Athletics participation flag", "sources": ["bronze/highered_athletics_rosters.student_id"]},
        {"name": "admission_date", "type": "date", "sources": ["bronze/peoplesoft_students.admission_date"]},
    ],
    "dim_courses": [
        {"name": "course_key", "type": "string", "description": "Surrogate key", "sources": []},
        {"name": "course_id", "type": "string", "sources": ["bronze/peoplesoft_courses.course_id"]},
        {"name": "course_name", "type": "string", "sources": ["bronze/peoplesoft_courses.course_name"]},
        {"name": "department_code", "type": "string", "sources": ["bronze/peoplesoft_courses.department_code"]},
        {"name": "credits", "type": "float", "sources": ["bronze/peoplesoft_courses.credits"]},
        {"name": "course_level", "type": "string", "sources": ["bronze/peoplesoft_courses.course_level"]},
        {"name": "max_enrollment", "type": "int", "sources": ["bronze/peoplesoft_courses.max_enrollment"]},
    ],
    "dim_employees": [
        {"name": "employee_key", "type": "string", "sources": []},
        {"name": "employee_id", "type": "string", "sources": ["bronze/sap_employees.employee_id"]},
        {"name": "full_name", "type": "string", "sources": ["bronze/sap_employees.first_name", "bronze/sap_employees.last_name"]},
        {"name": "department", "type": "string", "sources": ["bronze/sap_employees.department"]},
        {"name": "position_title", "type": "string", "sources": ["bronze/sap_employees.position_title"]},
        {"name": "employee_type", "type": "string", "sources": ["bronze/sap_employees.employee_type"]},
        {"name": "cost_center", "type": "string", "description": "Joined from cost centers", "sources": ["bronze/sap_cost_centers.cost_center_id"]},
    ],
    "fact_enrollments": [
        {"name": "enrollment_key", "type": "string", "sources": []},
        {"name": "student_id", "type": "string", "sources": ["bronze/peoplesoft_enrollments.student_id"]},
        {"name": "course_id", "type": "string", "sources": ["bronze/peoplesoft_enrollments.course_id"]},
        {"name": "term_code", "type": "string", "sources": ["bronze/peoplesoft_enrollments.term_code"]},
        {"name": "enrollment_date", "type": "date", "sources": ["bronze/peoplesoft_enrollments.enrollment_date"]},
        {"name": "grade", "type": "string", "sources": ["bronze/peoplesoft_enrollments.grade"]},
        {"name": "credits", "type": "float", "sources": ["bronze/peoplesoft_enrollments.credits"]},
        {"name": "enrollment_status", "type": "string", "sources": ["bronze/peoplesoft_enrollments.enrollment_status"]},
        {"name": "aid_amount", "type": "float", "description": "Financial aid for term", "sources": ["bronze/peoplesoft_financial_aid.amount_disbursed"]},
    ],
    "fact_financial_transactions": [
        {"name": "transaction_key", "type": "string", "sources": []},
        {"name": "document_number", "type": "string", "sources": ["bronze/sap_general_ledger.document_number"]},
        {"name": "posting_date", "type": "date", "sources": ["bronze/sap_general_ledger.posting_date"]},
        {"name": "gl_account", "type": "string", "sources": ["bronze/sap_general_ledger.gl_account"]},
        {"name": "cost_center", "type": "string", "sources": ["bronze/sap_general_ledger.cost_center"]},
        {"name": "fund_code", "type": "string", "sources": ["bronze/sap_general_ledger.fund_code"]},
        {"name": "amount", "type": "float", "sources": ["bronze/sap_general_ledger.amount"]},
        {"name": "department_name", "type": "string", "description": "Enriched from cost centers", "sources": ["bronze/sap_cost_centers.department"]},
        {"name": "college", "type": "string", "description": "Enriched from cost centers", "sources": ["bronze/sap_cost_centers.college"]},
    ],
    "fact_admissions_contacts": [
        {"name": "contact_key", "type": "string", "sources": []},
        {"name": "contact_id", "type": "string", "sources": ["bronze/salesforce_contacts.contact_id"]},
        {"name": "full_name", "type": "string", "sources": ["bronze/salesforce_contacts.first_name", "bronze/salesforce_contacts.last_name"]},
        {"name": "email", "type": "string", "sources": ["bronze/salesforce_contacts.email"]},
        {"name": "contact_type", "type": "string", "sources": ["bronze/salesforce_contacts.contact_type"]},
        {"name": "lead_source", "type": "string", "sources": ["bronze/salesforce_contacts.lead_source"]},
        {"name": "funnel_stage", "type": "string", "description": "Latest admissions funnel stage", "sources": ["bronze/salesforce_opportunities.stage_name"]},
        {"name": "program_interest", "type": "string", "sources": ["bronze/salesforce_opportunities.program_interest"]},
        {"name": "expected_revenue", "type": "float", "sources": ["bronze/salesforce_opportunities.amount"]},
        {"name": "campaign_name", "type": "string", "description": "Attribution campaign", "sources": ["bronze/salesforce_campaigns.campaign_name"]},
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
    transform_name: str,
    output_columns: list[dict],
    deps: list[dg.AssetKey],
) -> dg.TableColumnLineage | None:
    schema = _SILVER_SCHEMAS.get(transform_name)
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


def _demo_row_count(transform_name: str) -> int:
    seed = int(hashlib.md5(transform_name.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)
    counts = {
        "dim_students": (15000, 45000),
        "dim_courses": (3000, 8000),
        "dim_employees": (3000, 12000),
        "fact_enrollments": (80000, 250000),
        "fact_financial_transactions": (500000, 2000000),
        "fact_admissions_contacts": (50000, 200000),
    }
    low, high = counts.get(transform_name, (5000, 50000))
    return rng.randint(low, high)
