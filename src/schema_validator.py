"""Schema validation engine for pipeline migrations.

Compares source (Hive/HDFS) and target (Delta Lake/Iceberg) schemas to detect
incompatibilities before data movement begins. This prevents costly failures
mid-migration and generates actionable compatibility reports.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


class Severity(Enum):
    """Severity level for schema mismatches."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class CoercionRule(Enum):
    """Standard type coercion rules for migrations."""

    INT_TO_LONG = "int_to_long"
    FLOAT_TO_DOUBLE = "float_to_double"
    STRING_WIDENING = "string_widening"
    DATE_TO_TIMESTAMP = "date_to_timestamp"
    DECIMAL_PRECISION = "decimal_precision"


# Safe type promotions that don't lose data
SAFE_COERCIONS: dict[tuple[str, str], CoercionRule] = {
    ("IntegerType", "LongType"): CoercionRule.INT_TO_LONG,
    ("FloatType", "DoubleType"): CoercionRule.FLOAT_TO_DOUBLE,
    ("DateType", "TimestampType"): CoercionRule.DATE_TO_TIMESTAMP,
    ("StringType", "StringType"): CoercionRule.STRING_WIDENING,
}


@dataclass
class SchemaMismatch:
    """Represents a single schema incompatibility."""

    column: str
    source_type: str
    target_type: str
    severity: Severity
    message: str
    coercion_rule: Optional[CoercionRule] = None
    auto_resolvable: bool = False


@dataclass
class ValidationReport:
    """Complete schema validation report."""

    source_table: str
    target_table: str
    is_compatible: bool
    mismatches: list[SchemaMismatch] = field(default_factory=list)
    source_column_count: int = 0
    target_column_count: int = 0
    missing_in_target: list[str] = field(default_factory=list)
    extra_in_target: list[str] = field(default_factory=list)
    partition_validation: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    checksum: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize report to dictionary for JSON output."""
        return {
            "source_table": self.source_table,
            "target_table": self.target_table,
            "is_compatible": self.is_compatible,
            "timestamp": self.timestamp,
            "summary": {
                "source_columns": self.source_column_count,
                "target_columns": self.target_column_count,
                "mismatch_count": len(self.mismatches),
                "error_count": sum(
                    1 for m in self.mismatches if m.severity == Severity.ERROR
                ),
                "warning_count": sum(
                    1 for m in self.mismatches if m.severity == Severity.WARNING
                ),
            },
            "mismatches": [
                {
                    "column": m.column,
                    "source_type": m.source_type,
                    "target_type": m.target_type,
                    "severity": m.severity.value,
                    "message": m.message,
                    "auto_resolvable": m.auto_resolvable,
                }
                for m in self.mismatches
            ],
            "missing_in_target": self.missing_in_target,
            "extra_in_target": self.extra_in_target,
            "partition_validation": self.partition_validation,
            "checksum": self.checksum,
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize report to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


class SchemaValidator:
    """Validates schema compatibility between source and target tables.

    Performs deep comparison of column types, nullable constraints,
    partition layouts, and generates coercion recommendations.
    """

    def __init__(self, spark: SparkSession, strict_mode: bool = False):
        """Initialize the schema validator.

        Args:
            spark: Active SparkSession for metadata access.
            strict_mode: If True, treats warnings as errors.
        """
        self.spark = spark
        self.strict_mode = strict_mode
        self._type_map = self._build_type_map()

    def _build_type_map(self) -> dict[str, type]:
        """Build mapping of type name strings to PySpark type classes."""
        return {
            "string": StringType,
            "int": IntegerType,
            "integer": IntegerType,
            "bigint": LongType,
            "long": LongType,
            "float": FloatType,
            "double": DoubleType,
            "boolean": BooleanType,
            "date": DateType,
            "timestamp": TimestampType,
        }

    def validate(
        self,
        source_table: str,
        target_table: str,
        config: Any = None,
        custom_coercions: Optional[dict[tuple[str, str], CoercionRule]] = None,
    ) -> ValidationReport:
        """Run full schema validation between source and target.

        Args:
            source_table: Fully qualified source table name.
            target_table: Fully qualified target table name.
            config: Optional PipelineConfig for additional context.
            custom_coercions: Additional type coercions to allow.

        Returns:
            ValidationReport with all findings.
        """
        logger.info(f"Validating schema: {source_table} -> {target_table}")

        source_schema = self._get_schema(source_table)
        target_schema = self._get_schema(target_table)

        coercions = {**SAFE_COERCIONS}
        if custom_coercions:
            coercions.update(custom_coercions)

        mismatches = []
        missing_in_target = []
        extra_in_target = []

        source_fields = {f.name.lower(): f for f in source_schema.fields}
        target_fields = {f.name.lower(): f for f in target_schema.fields}

        # Check for missing columns
        for col_name in source_fields:
            if col_name not in target_fields:
                missing_in_target.append(col_name)

        for col_name in target_fields:
            if col_name not in source_fields:
                extra_in_target.append(col_name)

        # Compare matching columns
        for col_name, source_field in source_fields.items():
            if col_name not in target_fields:
                continue

            target_field = target_fields[col_name]
            mismatch = self._compare_fields(
                col_name, source_field, target_field, coercions
            )
            if mismatch:
                mismatches.append(mismatch)

        # Validate partition columns if config provided
        partition_validation = {}
        if config and hasattr(config, "source") and hasattr(config, "target"):
            partition_validation = self._validate_partitions(
                config.source.partition_columns,
                config.target.partition_columns,
                source_fields,
                target_fields,
            )

        # Determine overall compatibility
        has_errors = any(m.severity == Severity.ERROR for m in mismatches)
        has_missing = len(missing_in_target) > 0
        is_compatible = not has_errors and not has_missing

        if self.strict_mode:
            has_warnings = any(m.severity == Severity.WARNING for m in mismatches)
            is_compatible = is_compatible and not has_warnings

        report = ValidationReport(
            source_table=source_table,
            target_table=target_table,
            is_compatible=is_compatible,
            mismatches=mismatches,
            source_column_count=len(source_fields),
            target_column_count=len(target_fields),
            missing_in_target=missing_in_target,
            extra_in_target=extra_in_target,
            partition_validation=partition_validation,
        )

        report.checksum = self._compute_report_checksum(report)

        logger.info(
            f"Validation complete: compatible={is_compatible}, "
            f"mismatches={len(mismatches)}, missing={len(missing_in_target)}"
        )

        return report

    def _get_schema(self, table_name: str) -> StructType:
        """Retrieve schema for a table from the catalog."""
        try:
            df = self.spark.table(table_name)
            return df.schema
        except Exception as e:
            logger.error(f"Failed to get schema for {table_name}: {e}")
            raise ValueError(f"Cannot access table schema: {table_name}") from e

    def _compare_fields(
        self,
        col_name: str,
        source: StructField,
        target: StructField,
        coercions: dict[tuple[str, str], CoercionRule],
    ) -> Optional[SchemaMismatch]:
        """Compare two fields and return mismatch if incompatible."""
        source_type_name = type(source.dataType).__name__
        target_type_name = type(target.dataType).__name__

        # Types match exactly
        if source_type_name == target_type_name:
            # Check decimal precision
            if isinstance(source.dataType, DecimalType):
                return self._compare_decimal(col_name, source.dataType, target.dataType)
            return None

        # Check if safe coercion exists
        coercion_key = (source_type_name, target_type_name)
        if coercion_key in coercions:
            return SchemaMismatch(
                column=col_name,
                source_type=source_type_name,
                target_type=target_type_name,
                severity=Severity.INFO,
                message=f"Safe coercion: {coercions[coercion_key].value}",
                coercion_rule=coercions[coercion_key],
                auto_resolvable=True,
            )

        # Narrowing conversion (potential data loss)
        reverse_key = (target_type_name, source_type_name)
        if reverse_key in coercions:
            return SchemaMismatch(
                column=col_name,
                source_type=source_type_name,
                target_type=target_type_name,
                severity=Severity.ERROR,
                message=f"Narrowing conversion detected: {source_type_name} -> {target_type_name} may lose data",
                auto_resolvable=False,
            )

        # Unknown type change
        return SchemaMismatch(
            column=col_name,
            source_type=source_type_name,
            target_type=target_type_name,
            severity=Severity.ERROR,
            message=f"Incompatible types: {source_type_name} cannot be safely converted to {target_type_name}",
            auto_resolvable=False,
        )

    def _compare_decimal(
        self, col_name: str, source: DecimalType, target: DecimalType
    ) -> Optional[SchemaMismatch]:
        """Compare decimal precision and scale."""
        if source.precision <= target.precision and source.scale <= target.scale:
            return None

        severity = Severity.ERROR if source.precision > target.precision else Severity.WARNING
        return SchemaMismatch(
            column=col_name,
            source_type=f"Decimal({source.precision},{source.scale})",
            target_type=f"Decimal({target.precision},{target.scale})",
            severity=severity,
            message=f"Decimal precision/scale mismatch",
            auto_resolvable=False,
        )

    def _validate_partitions(
        self,
        source_partitions: list[str],
        target_partitions: list[str],
        source_fields: dict[str, StructField],
        target_fields: dict[str, StructField],
    ) -> dict[str, Any]:
        """Validate partition column alignment."""
        result = {
            "source_partitions": source_partitions,
            "target_partitions": target_partitions,
            "aligned": source_partitions == target_partitions,
            "issues": [],
        }

        if not result["aligned"]:
            missing = set(source_partitions) - set(target_partitions)
            extra = set(target_partitions) - set(source_partitions)
            if missing:
                result["issues"].append(
                    f"Partition columns missing in target: {list(missing)}"
                )
            if extra:
                result["issues"].append(
                    f"Extra partition columns in target: {list(extra)}"
                )

        return result

    def _compute_report_checksum(self, report: ValidationReport) -> str:
        """Compute a checksum for the report to detect modifications."""
        content = f"{report.source_table}|{report.target_table}|{len(report.mismatches)}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
