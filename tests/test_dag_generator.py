"""Unit tests for the DAG generator.

Tests cover:
- DAG file generation from config
- Correct task rendering for each load strategy
- Default args and scheduling configuration
- Task dependency chain correctness
- Slack alerting integration
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from src.config import (
    LoadStrategy,
    PipelineConfig,
    QualityConfig,
    ScheduleConfig,
    SourceConfig,
    TargetConfig,
    TargetFormat,
)
from src.dag_generator import DagGenerator


@pytest.fixture
def sample_config():
    """Create a sample pipeline config for testing."""
    return PipelineConfig(
        pipeline_id="test_pipeline_001",
        pipeline_name="Test Customer Events Migration",
        owner="data-team",
        load_strategy=LoadStrategy.INCREMENTAL,
        source=SourceConfig(
            database="legacy_db",
            table="customer_events",
            format="hive",
            connection_uri="thrift://metastore:9083",
            partition_columns=["event_date", "region"],
            watermark_column="event_date",
        ),
        target=TargetConfig(
            database="lakehouse",
            table="customer_events",
            format=TargetFormat.DELTA,
            path="s3://data-lake/lakehouse/customer_events",
            partition_columns=["event_date", "region"],
            z_order_columns=["customer_id"],
            write_mode="append",
        ),
        quality=QualityConfig(
            validate_row_count=True,
            row_count_tolerance_pct=0.01,
            validate_checksums=True,
            checksum_columns=["event_id", "customer_id"],
        ),
        schedule=ScheduleConfig(
            cron_expression="0 2 * * *",
            start_date="2024-01-01",
            retries=3,
            retry_delay_minutes=5,
            sla_minutes=120,
            slack_channel="#data-alerts",
        ),
        tags=["customer", "events"],
        description="Migrate customer events to Delta Lake",
    )


@pytest.fixture
def full_load_config(sample_config):
    """Config with full load strategy."""
    sample_config.load_strategy = LoadStrategy.FULL
    return sample_config


@pytest.fixture
def cdc_config(sample_config):
    """Config with CDC load strategy."""
    sample_config.load_strategy = LoadStrategy.CDC
    return sample_config


@pytest.fixture
def generator():
    """Create a DagGenerator instance."""
    return DagGenerator()


class TestDagGeneration:
    """Test DAG file creation."""

    def test_generates_dag_file(self, generator, sample_config):
        """Should create a .py file in the output directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = generator.generate_from_config(sample_config, output_dir=tmpdir)

            assert Path(result).exists()
            assert result.endswith(".py")
            assert "migrate_customer_events_dag.py" in result

    def test_dag_file_is_valid_python(self, generator, sample_config):
        """Generated DAG should be syntactically valid Python."""
        dag_code = generator.render_dag_code(sample_config)

        # Should not raise SyntaxError
        compile(dag_code, "<generated_dag>", "exec")

    def test_dag_id_format(self, generator, sample_config):
        """DAG ID should follow the naming convention."""
        dag_code = generator.render_dag_code(sample_config)

        assert 'dag_id="migrate_customer_events_dag"' in dag_code

    def test_dag_contains_pipeline_metadata(self, generator, sample_config):
        """Generated DAG should include pipeline metadata in docstring."""
        dag_code = generator.render_dag_code(sample_config)

        assert "test_pipeline_001" in dag_code
        assert "legacy_db.customer_events" in dag_code
        assert "lakehouse.customer_events" in dag_code


class TestTaskRendering:
    """Test individual task generation."""

    def test_incremental_load_task(self, generator, sample_config):
        """Incremental config should produce watermark-based task."""
        dag_code = generator.render_dag_code(sample_config)

        assert "migrate_data_incremental" in dag_code
        assert "--load-strategy" in dag_code
        assert "--watermark-column" in dag_code
        assert "event_date" in dag_code

    def test_full_load_task(self, generator, full_load_config):
        """Full load should use overwrite mode."""
        dag_code = generator.render_dag_code(full_load_config)

        assert "migrate_data_full" in dag_code
        assert '"overwrite"' in dag_code

    def test_cdc_load_task(self, generator, cdc_config):
        """CDC config should produce merge-based task."""
        dag_code = generator.render_dag_code(cdc_config)

        assert "migrate_data_cdc" in dag_code
        assert '"merge"' in dag_code

    def test_schema_validation_task_present(self, generator, sample_config):
        """Every DAG should include schema validation before migration."""
        dag_code = generator.render_dag_code(sample_config)

        assert "validate_schema" in dag_code
        assert "schema_validator.py" in dag_code

    def test_quality_check_task_present(self, generator, sample_config):
        """Every DAG should include post-migration quality checks."""
        dag_code = generator.render_dag_code(sample_config)

        assert "data_quality_check" in dag_code
        assert "quality_validator.py" in dag_code


class TestSchedulingConfig:
    """Test default args and schedule generation."""

    def test_retry_configuration(self, generator, sample_config):
        """Should include retry settings from config."""
        dag_code = generator.render_dag_code(sample_config)

        assert '"retries": 3' in dag_code
        assert "retry_delay" in dag_code
        assert "retry_exponential_backoff" in dag_code

    def test_schedule_interval(self, generator, sample_config):
        """Should use cron expression from config."""
        dag_code = generator.render_dag_code(sample_config)

        assert 'schedule_interval="0 2 * * *"' in dag_code

    def test_start_date_parsing(self, generator, sample_config):
        """Should correctly parse start_date string to datetime."""
        dag_code = generator.render_dag_code(sample_config)

        assert "datetime(2024, 1, 1)" in dag_code

    def test_catchup_disabled(self, generator, sample_config):
        """Catchup should be False for migration DAGs."""
        dag_code = generator.render_dag_code(sample_config)

        assert "catchup=False" in dag_code

    def test_max_active_runs_one(self, generator, sample_config):
        """Only one active run to prevent overlapping migrations."""
        dag_code = generator.render_dag_code(sample_config)

        assert "max_active_runs=1" in dag_code


class TestAlertingIntegration:
    """Test Slack and email alerting."""

    def test_slack_alert_task_when_configured(self, generator, sample_config):
        """Should include Slack alert task when channel is configured."""
        dag_code = generator.render_dag_code(sample_config)

        assert "alert_on_failure" in dag_code
        assert "SlackWebhookOperator" in dag_code
        assert "#data-alerts" in dag_code

    def test_no_slack_alert_when_not_configured(self, generator, sample_config):
        """Should NOT include Slack task when no channel configured."""
        sample_config.schedule.slack_channel = None
        dag_code = generator.render_dag_code(sample_config)

        assert "SlackWebhookOperator" not in dag_code

    def test_failure_trigger_rule(self, generator, sample_config):
        """Alert task should trigger on any upstream failure."""
        dag_code = generator.render_dag_code(sample_config)

        assert "TriggerRule.ONE_FAILED" in dag_code


class TestTaskDependencies:
    """Test the task dependency chain."""

    def test_linear_dependency_chain(self, generator, sample_config):
        """Tasks should follow: start >> validate >> migrate >> quality >> end."""
        dag_code = generator.render_dag_code(sample_config)

        assert "start >> validate_schema >> migrate_data >> quality_check >> end" in dag_code

    def test_alert_depends_on_all_tasks(self, generator, sample_config):
        """Alert should be downstream of all operational tasks."""
        dag_code = generator.render_dag_code(sample_config)

        assert "[validate_schema, migrate_data, quality_check] >> alert_on_failure" in dag_code


class TestEdgeCases:
    """Test edge cases and unusual configurations."""

    def test_table_name_with_special_chars(self, generator, sample_config):
        """Table names with dots/hyphens should be sanitized in DAG ID."""
        sample_config.source.table = "customer-events.v2"
        dag_code = generator.render_dag_code(sample_config)

        assert "migrate_customer_events_v2_dag" in dag_code

    def test_tags_included_in_dag(self, generator, sample_config):
        """Pipeline tags should be included in DAG tags."""
        dag_code = generator.render_dag_code(sample_config)

        assert '"customer"' in dag_code
        assert '"events"' in dag_code
        assert '"migration"' in dag_code
        assert '"auto-generated"' in dag_code

    def test_output_directory_created(self, generator, sample_config):
        """Should create output directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_dir = os.path.join(tmpdir, "sub", "dags")
            generator.generate_from_config(sample_config, output_dir=nested_dir)

            assert os.path.isdir(nested_dir)
