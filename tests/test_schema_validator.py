"""Unit tests for the schema validation engine."""

import json
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.schema_validator import (
    SchemaValidator,
    Severity,
    ValidationReport,
)


@pytest.fixture
def mock_spark():
    """Create a mock SparkSession for unit testing."""
    return MagicMock()


@pytest.fixture
def validator(mock_spark):
    """Create a SchemaValidator instance with mocked Spark."""
    return SchemaValidator(mock_spark)


class TestTypeCompatibility:
    """Test type promotion and mismatch detection."""

    def test_identical_schemas_are_compatible(self, validator, mock_spark):
        """Exact same schema should produce zero mismatches."""
        schema = StructType([
            StructField("id", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
        ])

        mock_df = MagicMock()
        mock_df.schema = schema
        mock_spark.table.return_value = mock_df

        report = validator.validate("db.source", "db.target")

        assert report.is_compatible is True
        assert len(report.mismatches) == 0

    def test_safe_integer_to_long_promotion(self, validator, mock_spark):
        """int to long is a safe widening - should be INFO, not ERROR."""
        source_schema = StructType([
            StructField("user_id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
        ])
        target_schema = StructType([
            StructField("user_id", LongType(), nullable=False),
            StructField("name", StringType(), nullable=True),
        ])

        mock_source_df = MagicMock()
        mock_source_df.schema = source_schema
        mock_target_df = MagicMock()
        mock_target_df.schema = target_schema
        mock_spark.table.side_effect = [mock_source_df, mock_target_df]

        report = validator.validate("db.source", "db.target")

        assert report.is_compatible is True
        assert len(report.mismatches) == 1
        assert report.mismatches[0].severity == Severity.INFO
        assert report.mismatches[0].auto_resolvable is True

    def test_float_to_double_promotion(self, validator, mock_spark):
        """float to double is safe type widening."""
        source_schema = StructType([
            StructField("price", FloatType(), nullable=True),
        ])
        target_schema = StructType([
            StructField("price", DoubleType(), nullable=True),
        ])

        mock_source_df = MagicMock()
        mock_source_df.schema = source_schema
        mock_target_df = MagicMock()
        mock_target_df.schema = target_schema
        mock_spark.table.side_effect = [mock_source_df, mock_target_df]

        report = validator.validate("db.source", "db.target")

        assert report.is_compatible is True
        assert report.mismatches[0].auto_resolvable is True

    def test_long_to_integer_narrowing_is_error(self, validator, mock_spark):
        """long to int is narrowing (potential overflow) - should be ERROR."""
        source_schema = StructType([
            StructField("big_id", LongType(), nullable=False),
        ])
        target_schema = StructType([
            StructField("big_id", IntegerType(), nullable=False),
        ])

        mock_source_df = MagicMock()
        mock_source_df.schema = source_schema
        mock_target_df = MagicMock()
        mock_target_df.schema = target_schema
        mock_spark.table.side_effect = [mock_source_df, mock_target_df]

        report = validator.validate("db.source", "db.target")

        assert report.is_compatible is False
        assert report.mismatches[0].severity == Severity.ERROR

    def test_string_to_integer_is_type_mismatch(self, validator, mock_spark):
        """string to integer is not a simple promotion."""
        source_schema = StructType([
            StructField("status_code", StringType(), nullable=True),
        ])
        target_schema = StructType([
            StructField("status_code", IntegerType(), nullable=True),
        ])

        mock_source_df = MagicMock()
        mock_source_df.schema = source_schema
        mock_target_df = MagicMock()
        mock_target_df.schema = target_schema
        mock_spark.table.side_effect = [mock_source_df, mock_target_df]

        report = validator.validate("db.source", "db.target")

        assert report.is_compatible is False
        assert report.mismatches[0].severity == Severity.ERROR

    def test_date_to_timestamp_promotion(self, validator, mock_spark):
        """date to timestamp is a safe promotion."""
        source_schema = StructType([
            StructField("event_date", DateType(), nullable=True),
        ])
        target_schema = StructType([
            StructField("event_date", TimestampType(), nullable=True),
        ])

        mock_source_df = MagicMock()
        mock_source_df.schema = source_schema
        mock_target_df = MagicMock()
        mock_target_df.schema = target_schema
        mock_spark.table.side_effect = [mock_source_df, mock_target_df]

        report = validator.validate("db.source", "db.target")

        assert report.is_compatible is True


class TestColumnPresence:
    """Test detection of missing and extra columns."""

    def test_missing_column_in_target(self, validator, mock_spark):
        """Source columns missing in target means data loss."""
        source_schema = StructType([
            StructField("id", LongType(), nullable=False),
            StructField("email", StringType(), nullable=True),
            StructField("legacy_field", StringType(), nullable=True),
        ])
        target_schema = StructType([
            StructField("id", LongType(), nullable=False),
            StructField("email", StringType(), nullable=True),
        ])

        mock_source_df = MagicMock()
        mock_source_df.schema = source_schema
        mock_target_df = MagicMock()
        mock_target_df.schema = target_schema
        mock_spark.table.side_effect = [mock_source_df, mock_target_df]

        report = validator.validate("db.source", "db.target")

        assert report.is_compatible is False
        assert "legacy_field" in report.missing_in_target

    def test_extra_column_in_target_is_ok(self, validator, mock_spark):
        """Extra columns in target are fine."""
        source_schema = StructType([
            StructField("id", LongType(), nullable=False),
        ])
        target_schema = StructType([
            StructField("id", LongType(), nullable=False),
            StructField("new_feature_flag", BooleanType(), nullable=True),
        ])

        mock_source_df = MagicMock()
        mock_source_df.schema = source_schema
        mock_target_df = MagicMock()
        mock_target_df.schema = target_schema
        mock_spark.table.side_effect = [mock_source_df, mock_target_df]

        report = validator.validate("db.source", "db.target")

        assert report.is_compatible is True
        assert "new_feature_flag" in report.extra_in_target


class TestReportSerialization:
    """Test report JSON output."""

    def test_report_to_json(self, validator, mock_spark):
        """Validation report should serialize to JSON."""
        source_schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("value", FloatType(), nullable=True),
        ])
        target_schema = StructType([
            StructField("id", LongType(), nullable=False),
            StructField("value", DoubleType(), nullable=True),
        ])

        mock_source_df = MagicMock()
        mock_source_df.schema = source_schema
        mock_target_df = MagicMock()
        mock_target_df.schema = target_schema
        mock_spark.table.side_effect = [mock_source_df, mock_target_df]

        report = validator.validate("src_db.events", "lake.events")
        json_str = report.to_json()
        parsed = json.loads(json_str)

        assert parsed["source_table"] == "src_db.events"
        assert parsed["target_table"] == "lake.events"
        assert "mismatches" in parsed
