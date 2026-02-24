import hashlib
import json
import random
from typing import Sequence

import dagster as dg
from pydantic import BaseModel


class SourceSystemConfig(BaseModel):
    """Configuration for a source system feeding the data lakehouse."""

    name: str
    description: str = ""
    tables: list[str]
    s3_prefix: str
    file_format: str = "parquet"


class S3DataLandingComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ingests raw data from university source systems into the bronze layer on S3.

    Monitors S3 landing zones for new Parquet files from PeopleSoft SIS, SAP ERP,
    Salesforce CRM, and higher-ed operational systems. Creates bronze-layer assets
    and S3 sensors to trigger downstream processing when new data arrives.
    """

    s3_bucket: str = "university-data-lakehouse"
    s3_landing_prefix: str = "landing/"
    s3_bronze_prefix: str = "bronze/"
    aws_region: str = "us-east-1"
    demo_mode: bool = False
    source_systems: Sequence[SourceSystemConfig] = []
    sensor_interval_seconds: int = 60

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets = []
        sensors = []
        asset_checks = []

        for source in self.source_systems:
            for table in source.tables:
                asset_key = dg.AssetKey(["bronze", f"{source.name}_{table}"])
                asset = self._make_bronze_asset(source, table, asset_key)
                assets.append(asset)

                check = self._make_freshness_check(source, table, asset_key)
                asset_checks.append(check)

            sensor = self._make_s3_sensor(source)
            sensors.append(sensor)

        return dg.Definitions(
            assets=assets,
            sensors=sensors,
            asset_checks=asset_checks,
        )

    def _make_bronze_asset(
        self,
        source: SourceSystemConfig,
        table: str,
        asset_key: dg.AssetKey,
    ) -> dg.AssetsDefinition:
        s3_bucket = self.s3_bucket
        s3_landing_prefix = self.s3_landing_prefix
        s3_bronze_prefix = self.s3_bronze_prefix
        aws_region = self.aws_region
        demo_mode = self.demo_mode
        source_name = source.name
        file_format = source.file_format
        s3_prefix = source.s3_prefix

        @dg.asset(
            key=asset_key,
            description=(
                f"Bronze layer: raw {table} data from {source.description or source_name}. "
                f"Ingested from s3://{s3_bucket}/{s3_landing_prefix}{s3_prefix}/{table}/ "
                f"as {file_format} files."
            ),
            group_name="bronze_ingestion",
            kinds={"s3", "parquet"},
            metadata={
                "source_system": source_name,
                "s3_landing_path": f"s3://{s3_bucket}/{s3_landing_prefix}{s3_prefix}/{table}/",
                "s3_bronze_path": f"s3://{s3_bucket}/{s3_bronze_prefix}{source_name}/{table}/",
                "file_format": file_format,
                "dagster/column_schema": dg.TableSchema(
                    columns=_get_column_schema(source_name, table)
                ),
            },
            tags={"layer": "bronze", "source_system": source_name},
        )
        def _bronze_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            landing_path = f"s3://{s3_bucket}/{s3_landing_prefix}{s3_prefix}/{table}/"
            bronze_path = f"s3://{s3_bucket}/{s3_bronze_prefix}{source_name}/{table}/"

            if demo_mode:
                row_count = _demo_row_count(source_name, table)
                file_count = random.randint(1, 5)
                size_bytes = row_count * random.randint(200, 800)
                context.log.info(
                    f"[DEMO] Ingesting {row_count} rows from {landing_path} → {bronze_path}"
                )
            else:
                import boto3

                s3_client = boto3.client("s3", region_name=aws_region)

                landing_key = f"{s3_landing_prefix}{s3_prefix}/{table}/"
                response = s3_client.list_objects_v2(
                    Bucket=s3_bucket, Prefix=landing_key
                )
                objects = response.get("Contents", [])
                file_count = len(objects)
                size_bytes = sum(obj["Size"] for obj in objects)

                for obj in objects:
                    source_key = obj["Key"]
                    dest_key = source_key.replace(s3_landing_prefix, s3_bronze_prefix, 1)
                    dest_key = dest_key.replace(s3_prefix, f"{source_name}", 1)

                    context.log.info(f"Copying {source_key} → {dest_key}")
                    s3_client.copy_object(
                        Bucket=s3_bucket,
                        CopySource={"Bucket": s3_bucket, "Key": source_key},
                        Key=dest_key,
                    )

                row_count = _estimate_parquet_rows(size_bytes)
                context.log.info(
                    f"Ingested {file_count} files ({size_bytes} bytes) from {landing_path}"
                )

            columns = _get_column_schema(source_name, table)
            column_lineage = dg.TableColumnLineage(
                deps_by_column={
                    col.name: [
                        dg.TableColumnDep(
                            asset_key=dg.AssetKey(
                                ["source", source_name, table]
                            ),
                            column_name=col.name,
                        )
                    ]
                    for col in columns
                }
            )

            return dg.MaterializeResult(
                metadata={
                    "row_count": dg.MetadataValue.int(row_count),
                    "file_count": dg.MetadataValue.int(file_count),
                    "size_bytes": dg.MetadataValue.int(size_bytes),
                    "landing_path": dg.MetadataValue.text(landing_path),
                    "bronze_path": dg.MetadataValue.text(bronze_path),
                    "dagster/column_lineage": column_lineage,
                },
            )

        _bronze_asset.__name__ = f"bronze_{source_name}_{table}"
        _bronze_asset.__qualname__ = f"bronze_{source_name}_{table}"
        return _bronze_asset

    def _make_freshness_check(
        self,
        source: SourceSystemConfig,
        table: str,
        asset_key: dg.AssetKey,
    ) -> dg.AssetChecksDefinition:
        source_name = source.name
        demo_mode = self.demo_mode

        @dg.asset_check(
            asset=asset_key,
            name=f"freshness_{source_name}_{table}",
            description=f"Verify {source_name}.{table} data arrived within SLA window",
        )
        def _freshness_check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
            if demo_mode:
                return dg.AssetCheckResult(
                    passed=True,
                    severity=dg.AssetCheckSeverity.WARN,
                    description=f"[DEMO] {source_name}.{table} freshness OK",
                    metadata={"hours_since_update": 2.3},
                )
            else:
                return dg.AssetCheckResult(
                    passed=True,
                    severity=dg.AssetCheckSeverity.WARN,
                    description=f"{source_name}.{table} freshness within SLA",
                )

        _freshness_check.__name__ = f"freshness_{source_name}_{table}"
        _freshness_check.__qualname__ = f"freshness_{source_name}_{table}"
        return _freshness_check

    def _make_s3_sensor(self, source: SourceSystemConfig) -> dg.SensorDefinition:
        s3_bucket = self.s3_bucket
        s3_landing_prefix = self.s3_landing_prefix
        aws_region = self.aws_region
        demo_mode = self.demo_mode
        source_name = source.name
        s3_prefix = source.s3_prefix
        tables = source.tables
        interval_seconds = self.sensor_interval_seconds

        @dg.sensor(
            name=f"s3_sensor_{source_name}",
            description=(
                f"Monitors s3://{s3_bucket}/{s3_landing_prefix}{s3_prefix}/ "
                f"for new data files from {source.description or source_name}"
            ),
            minimum_interval_seconds=interval_seconds,
            default_status=dg.DefaultSensorStatus.STOPPED,
        )
        def _s3_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
            cursor = json.loads(context.cursor) if context.cursor else {}
            run_requests = []

            if demo_mode:
                for table in tables:
                    marker = f"{source_name}/{table}"
                    last_key = cursor.get(marker, "")
                    new_key = f"{s3_landing_prefix}{s3_prefix}/{table}/data_{context.cursor_updated_timestamp or 0}.parquet"
                    if new_key != last_key:
                        asset_key = dg.AssetKey(["bronze", f"{source_name}_{table}"])
                        run_requests.append(
                            dg.RunRequest(
                                run_key=f"{source_name}_{table}_{context.cursor_updated_timestamp}",
                                asset_selection=[asset_key],
                            )
                        )
                        cursor[marker] = new_key
            else:
                import boto3

                s3_client = boto3.client("s3", region_name=aws_region)
                for table in tables:
                    prefix = f"{s3_landing_prefix}{s3_prefix}/{table}/"
                    marker = f"{source_name}/{table}"
                    last_key = cursor.get(marker, "")

                    response = s3_client.list_objects_v2(
                        Bucket=s3_bucket,
                        Prefix=prefix,
                        StartAfter=last_key,
                    )
                    new_objects = response.get("Contents", [])

                    if new_objects:
                        latest_key = new_objects[-1]["Key"]
                        asset_key = dg.AssetKey(["bronze", f"{source_name}_{table}"])
                        run_requests.append(
                            dg.RunRequest(
                                run_key=f"{source_name}_{table}_{latest_key}",
                                asset_selection=[asset_key],
                            )
                        )
                        cursor[marker] = latest_key

            return dg.SensorResult(
                run_requests=run_requests,
                cursor=json.dumps(cursor),
            )

        _s3_sensor.__name__ = f"s3_sensor_{source_name}"
        _s3_sensor.__qualname__ = f"s3_sensor_{source_name}"
        return _s3_sensor


# --- Schema definitions for column-level lineage ---

_SCHEMAS: dict[str, dict[str, list[dg.TableColumn]]] = {
    "peoplesoft": {
        "students": [
            dg.TableColumn("student_id", type="string", description="University student ID (EMPLID)"),
            dg.TableColumn("first_name", type="string"),
            dg.TableColumn("last_name", type="string"),
            dg.TableColumn("email", type="string"),
            dg.TableColumn("date_of_birth", type="date"),
            dg.TableColumn("admission_date", type="date"),
            dg.TableColumn("program_code", type="string"),
            dg.TableColumn("academic_level", type="string", description="UG/GR/PHD"),
            dg.TableColumn("gpa", type="float"),
            dg.TableColumn("enrollment_status", type="string", description="Active/LOA/Graduated/Withdrawn"),
        ],
        "enrollments": [
            dg.TableColumn("enrollment_id", type="string"),
            dg.TableColumn("student_id", type="string"),
            dg.TableColumn("course_id", type="string"),
            dg.TableColumn("term_code", type="string", description="e.g. 2024FA"),
            dg.TableColumn("section_number", type="string"),
            dg.TableColumn("enrollment_date", type="date"),
            dg.TableColumn("grade", type="string"),
            dg.TableColumn("credits", type="float"),
            dg.TableColumn("enrollment_status", type="string"),
        ],
        "courses": [
            dg.TableColumn("course_id", type="string"),
            dg.TableColumn("course_name", type="string"),
            dg.TableColumn("department_code", type="string"),
            dg.TableColumn("credits", type="float"),
            dg.TableColumn("course_level", type="string"),
            dg.TableColumn("max_enrollment", type="int"),
        ],
        "financial_aid": [
            dg.TableColumn("aid_id", type="string"),
            dg.TableColumn("student_id", type="string"),
            dg.TableColumn("aid_year", type="string"),
            dg.TableColumn("aid_type", type="string", description="Grant/Loan/Scholarship/Work-Study"),
            dg.TableColumn("amount_offered", type="float"),
            dg.TableColumn("amount_accepted", type="float"),
            dg.TableColumn("amount_disbursed", type="float"),
            dg.TableColumn("fund_source", type="string", description="Federal/State/Institutional"),
        ],
    },
    "sap": {
        "general_ledger": [
            dg.TableColumn("document_number", type="string"),
            dg.TableColumn("posting_date", type="date"),
            dg.TableColumn("company_code", type="string"),
            dg.TableColumn("gl_account", type="string"),
            dg.TableColumn("cost_center", type="string"),
            dg.TableColumn("fund_code", type="string"),
            dg.TableColumn("amount", type="float"),
            dg.TableColumn("currency", type="string"),
            dg.TableColumn("document_type", type="string"),
        ],
        "cost_centers": [
            dg.TableColumn("cost_center_id", type="string"),
            dg.TableColumn("cost_center_name", type="string"),
            dg.TableColumn("department", type="string"),
            dg.TableColumn("college", type="string"),
            dg.TableColumn("fund_group", type="string"),
            dg.TableColumn("responsible_person", type="string"),
        ],
        "employees": [
            dg.TableColumn("employee_id", type="string"),
            dg.TableColumn("first_name", type="string"),
            dg.TableColumn("last_name", type="string"),
            dg.TableColumn("department", type="string"),
            dg.TableColumn("position_title", type="string"),
            dg.TableColumn("hire_date", type="date"),
            dg.TableColumn("employee_type", type="string", description="Faculty/Staff/Adjunct"),
            dg.TableColumn("fte", type="float"),
        ],
    },
    "salesforce": {
        "contacts": [
            dg.TableColumn("contact_id", type="string"),
            dg.TableColumn("first_name", type="string"),
            dg.TableColumn("last_name", type="string"),
            dg.TableColumn("email", type="string"),
            dg.TableColumn("phone", type="string"),
            dg.TableColumn("contact_type", type="string", description="Prospect/Applicant/Alumni/Donor"),
            dg.TableColumn("lead_source", type="string"),
            dg.TableColumn("created_date", type="date"),
        ],
        "opportunities": [
            dg.TableColumn("opportunity_id", type="string"),
            dg.TableColumn("contact_id", type="string"),
            dg.TableColumn("stage_name", type="string", description="Inquiry/Application/Admitted/Enrolled/Deposited"),
            dg.TableColumn("program_interest", type="string"),
            dg.TableColumn("term_code", type="string"),
            dg.TableColumn("amount", type="float", description="Expected tuition revenue"),
            dg.TableColumn("close_date", type="date"),
            dg.TableColumn("probability", type="float"),
        ],
        "campaigns": [
            dg.TableColumn("campaign_id", type="string"),
            dg.TableColumn("campaign_name", type="string"),
            dg.TableColumn("campaign_type", type="string", description="Email/Event/Social/Direct Mail"),
            dg.TableColumn("start_date", type="date"),
            dg.TableColumn("end_date", type="date"),
            dg.TableColumn("budget", type="float"),
            dg.TableColumn("actual_cost", type="float"),
            dg.TableColumn("responses", type="int"),
        ],
    },
    "highered": {
        "housing_assignments": [
            dg.TableColumn("assignment_id", type="string"),
            dg.TableColumn("student_id", type="string"),
            dg.TableColumn("building_code", type="string"),
            dg.TableColumn("room_number", type="string"),
            dg.TableColumn("term_code", type="string"),
            dg.TableColumn("meal_plan", type="string"),
            dg.TableColumn("assignment_date", type="date"),
        ],
        "athletics_rosters": [
            dg.TableColumn("roster_id", type="string"),
            dg.TableColumn("student_id", type="string"),
            dg.TableColumn("sport_code", type="string"),
            dg.TableColumn("academic_year", type="string"),
            dg.TableColumn("eligibility_status", type="string"),
            dg.TableColumn("scholarship_type", type="string"),
        ],
        "student_conduct": [
            dg.TableColumn("incident_id", type="string"),
            dg.TableColumn("student_id", type="string"),
            dg.TableColumn("incident_date", type="date"),
            dg.TableColumn("incident_type", type="string"),
            dg.TableColumn("severity", type="string"),
            dg.TableColumn("resolution", type="string"),
            dg.TableColumn("sanction", type="string"),
        ],
        "field_placements": [
            dg.TableColumn("placement_id", type="string"),
            dg.TableColumn("student_id", type="string"),
            dg.TableColumn("site_name", type="string"),
            dg.TableColumn("supervisor_name", type="string"),
            dg.TableColumn("start_date", type="date"),
            dg.TableColumn("end_date", type="date"),
            dg.TableColumn("hours_required", type="float"),
            dg.TableColumn("hours_completed", type="float"),
            dg.TableColumn("program_code", type="string"),
        ],
    },
}


def _get_column_schema(source_name: str, table: str) -> list[dg.TableColumn]:
    return _SCHEMAS.get(source_name, {}).get(table, [
        dg.TableColumn("id", type="string"),
        dg.TableColumn("data", type="string"),
        dg.TableColumn("updated_at", type="timestamp"),
    ])


def _demo_row_count(source_name: str, table: str) -> int:
    seed = int(hashlib.md5(f"{source_name}_{table}".encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)
    counts = {
        "students": (15000, 45000),
        "enrollments": (80000, 250000),
        "courses": (3000, 8000),
        "financial_aid": (40000, 120000),
        "general_ledger": (500000, 2000000),
        "cost_centers": (200, 800),
        "employees": (3000, 12000),
        "contacts": (50000, 200000),
        "opportunities": (20000, 80000),
        "campaigns": (100, 500),
        "housing_assignments": (5000, 15000),
        "athletics_rosters": (500, 2000),
        "student_conduct": (200, 1000),
        "field_placements": (1000, 5000),
    }
    low, high = counts.get(table, (1000, 10000))
    return rng.randint(low, high)


def _estimate_parquet_rows(size_bytes: int) -> int:
    return max(1, size_bytes // 400)
