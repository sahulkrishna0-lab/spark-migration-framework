"""Configuration management for pipeline migrations.

Handles YAML-based pipeline configs with validation, defaults,
and environment-specific overrides.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import yaml


class LoadStrategy(Enum):
    """Data loading strategy for the migration."""

    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"


class TargetFormat(Enum):
    """Target table format."""

    DELTA = "delta"
    ICEBERG = "iceberg"
    PARQUET = "parquet"


@dataclass
class SourceConfig:
    """Source system configuration."""

    database: str
    table: str
    format: str = "hive"
    connection_uri: str = ""
    partition_columns: list[str] = field(default_factory=list)
    watermark_column: Optional[str] = None
    custom_query: Optional[str] = None


@dataclass
class TargetConfig:
    """Target system configuration."""

    database: str
    table: str
    format: TargetFormat = TargetFormat.DELTA
    path: str = ""
    partition_columns: list[str] = field(default_factory=list)
    z_order_columns: list[str] = field(default_factory=list)
    write_mode: str = "append"


@dataclass
class QualityConfig:
    """Data quality validation settings."""

    validate_row_count: bool = True
    row_count_tolerance_pct: float = 0.01
    validate_checksums: bool = True
    checksum_columns: list[str] = field(default_factory=list)
    validate_nulls: bool = True
    null_threshold_pct: float = 5.0


@dataclass
class ScheduleConfig:
    """Airflow scheduling configuration."""

    cron_expression: str = "0 2 * * *"
    start_date: str = "2024-01-01"
    retries: int = 3
    retry_delay_minutes: int = 5
    sla_minutes: int = 120
    alert_emails: list[str] = field(default_factory=list)
    slack_channel: Optional[str] = None


@dataclass
class PipelineConfig:
    """Complete pipeline migration configuration."""

    pipeline_id: str
    pipeline_name: str
    owner: str
    load_strategy: LoadStrategy
    source: SourceConfig
    target: TargetConfig
    quality: QualityConfig = field(default_factory=QualityConfig)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    tags: list[str] = field(default_factory=list)
    description: str = ""

    @classmethod
    def from_yaml(cls, config_path: str | Path) -> PipelineConfig:
        """Load pipeline configuration from a YAML file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path, "r") as f:
            raw = yaml.safe_load(f)

        raw = cls._apply_env_overrides(raw)
        return cls._parse_config(raw)

    @classmethod
    def _apply_env_overrides(cls, raw: dict[str, Any]) -> dict[str, Any]:
        """Replace ${ENV_VAR} placeholders with environment values."""
        def _resolve(value: Any) -> Any:
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_key = value[2:-1]
                return os.environ.get(env_key, value)
            elif isinstance(value, dict):
                return {k: _resolve(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [_resolve(item) for item in value]
            return value

        return _resolve(raw)

    @classmethod
    def _parse_config(cls, raw: dict[str, Any]) -> PipelineConfig:
        """Parse raw YAML dict into typed PipelineConfig."""
        pipeline = raw.get("pipeline", {})

        source_raw = pipeline.get("source", {})
        source = SourceConfig(
            database=source_raw["database"],
            table=source_raw["table"],
            format=source_raw.get("format", "hive"),
            connection_uri=source_raw.get("connection_uri", ""),
            partition_columns=source_raw.get("partition_columns", []),
            watermark_column=source_raw.get("watermark_column"),
            custom_query=source_raw.get("custom_query"),
        )

        target_raw = pipeline.get("target", {})
        target = TargetConfig(
            database=target_raw["database"],
            table=target_raw["table"],
            format=TargetFormat(target_raw.get("format", "delta")),
            path=target_raw.get("path", ""),
            partition_columns=target_raw.get("partition_columns", []),
            z_order_columns=target_raw.get("z_order_columns", []),
            write_mode=target_raw.get("write_mode", "append"),
        )

        quality_raw = pipeline.get("quality", {})
        quality = QualityConfig(
            validate_row_count=quality_raw.get("validate_row_count", True),
            row_count_tolerance_pct=quality_raw.get("row_count_tolerance_pct", 0.01),
            validate_checksums=quality_raw.get("validate_checksums", True),
            checksum_columns=quality_raw.get("checksum_columns", []),
            validate_nulls=quality_raw.get("validate_nulls", True),
            null_threshold_pct=quality_raw.get("null_threshold_pct", 5.0),
        )

        schedule_raw = pipeline.get("schedule", {})
        schedule = ScheduleConfig(
            cron_expression=schedule_raw.get("cron_expression", "0 2 * * *"),
            start_date=schedule_raw.get("start_date", "2024-01-01"),
            retries=schedule_raw.get("retries", 3),
            retry_delay_minutes=schedule_raw.get("retry_delay_minutes", 5),
            sla_minutes=schedule_raw.get("sla_minutes", 120),
            alert_emails=schedule_raw.get("alert_emails", []),
            slack_channel=schedule_raw.get("slack_channel"),
        )

        return cls(
            pipeline_id=pipeline["pipeline_id"],
            pipeline_name=pipeline["pipeline_name"],
            owner=pipeline.get("owner", "data-engineering"),
            load_strategy=LoadStrategy(pipeline.get("load_strategy", "full")),
            source=source,
            target=target,
            quality=quality,
            schedule=schedule,
            tags=pipeline.get("tags", []),
            description=pipeline.get("description", ""),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize config back to a dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "owner": self.owner,
            "load_strategy": self.load_strategy.value,
            "source": {
                "database": self.source.database,
                "table": self.source.table,
                "format": self.source.format,
            },
            "target": {
                "database": self.target.database,
                "table": self.target.table,
                "format": self.target.format.value,
            },
            "tags": self.tags,
        }
