from typing import Sequence

import dagster as dg
from pydantic import BaseModel


class ScheduledJobConfig(BaseModel):
    """Configuration for a scheduled job targeting assets by selection."""

    name: str
    cron_schedule: str
    asset_selection: str
    description: str = ""
    default_status: str = "STOPPED"
    execution_timezone: str = "US/Eastern"
    tags: dict[str, str] = {}


class ScheduledJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Creates scheduled jobs that target assets using Dagster's asset selection syntax.

    Supports selection by group, tag, kind, owner, or explicit asset keys.
    Designed for university data lakehouse scheduling patterns across
    bronze ingestion, silver transformation, and gold analytics layers.
    """

    jobs: Sequence[ScheduledJobConfig] = []

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        jobs = []
        schedules = []

        for job_config in self.jobs:
            selection = _parse_asset_selection(job_config.asset_selection)

            job = dg.define_asset_job(
                name=job_config.name,
                selection=selection,
                description=job_config.description,
                tags=job_config.tags,
            )
            jobs.append(job)

            default_status = (
                dg.DefaultScheduleStatus.RUNNING
                if job_config.default_status == "RUNNING"
                else dg.DefaultScheduleStatus.STOPPED
            )

            schedule = dg.ScheduleDefinition(
                name=f"{job_config.name}_schedule",
                job=job,
                cron_schedule=job_config.cron_schedule,
                execution_timezone=job_config.execution_timezone,
                default_status=default_status,
            )
            schedules.append(schedule)

        return dg.Definitions(jobs=jobs, schedules=schedules)


def _parse_asset_selection(selection_str: str) -> dg.AssetSelection:
    """Parse a selection string into a Dagster AssetSelection.

    Supports:
      - "group:name" → AssetSelection.groups("name")
      - "tag:key=value" → AssetSelection.tag("key", "value")
      - "tag:key" → AssetSelection.tag("key", "")
      - "kind:name" → AssetSelection.tag("dagster/kind/name", "")
      - "key:a/b" → AssetSelection.keys(AssetKey(["a", "b"]))
      - "*" → AssetSelection.all()
      - "sel1 | sel2" → union
      - "sel1 & sel2" → intersection
    """
    selection_str = selection_str.strip()

    if "|" in selection_str:
        parts = [p.strip() for p in selection_str.split("|")]
        result = _parse_asset_selection(parts[0])
        for part in parts[1:]:
            result = result | _parse_asset_selection(part)
        return result

    if "&" in selection_str:
        parts = [p.strip() for p in selection_str.split("&")]
        result = _parse_asset_selection(parts[0])
        for part in parts[1:]:
            result = result & _parse_asset_selection(part)
        return result

    if selection_str == "*":
        return dg.AssetSelection.all()

    if selection_str.startswith("group:"):
        group_name = selection_str[len("group:"):]
        return dg.AssetSelection.groups(group_name)

    if selection_str.startswith("tag:"):
        tag_expr = selection_str[len("tag:"):]
        if "=" in tag_expr:
            key, value = tag_expr.split("=", 1)
            return dg.AssetSelection.tag(key, value)
        return dg.AssetSelection.tag(tag_expr, "")

    if selection_str.startswith("kind:"):
        kind_name = selection_str[len("kind:"):]
        return dg.AssetSelection.tag(f"dagster/kind/{kind_name}", "")

    if selection_str.startswith("key:"):
        key_path = selection_str[len("key:"):]
        return dg.AssetSelection.keys(dg.AssetKey(key_path.split("/")))

    return dg.AssetSelection.groups(selection_str)
