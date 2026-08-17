# 🚀 Spark Pipeline Migration Framework

[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://python.org)
[![PySpark 3.4+](https://img.shields.io/badge/PySpark-3.4%2B-orange.svg)](https://spark.apache.org)
[![Apache Airflow 2.x](https://img.shields.io/badge/Airflow-2.x-017CEE.svg)](https://airflow.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4-00ADD8.svg)](https://delta.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> A reusable framework for migrating legacy Hadoop/Spark ETL pipelines to cloud-native lakehouse architecture. Automates schema validation, DAG generation, and zero-downtime cutover for 50+ pipelines.

---

## 📋 Overview

Enterprise data teams often manage hundreds of legacy Hadoop pipelines that need modernization. This framework eliminates the manual toil of migration by providing:

- **Automated schema validation** between source (Hive/HDFS) and target (Delta Lake/Iceberg)
- **DAG generation** — convert pipeline configs to production-ready Airflow DAGs
- **Data integrity verification** — row counts, checksums, and column-level validation
- **Checkpointed execution** — resume failed migrations without re-processing

Built from real-world experience migrating 50+ pipelines from on-prem Hadoop clusters to AWS/Databricks lakehouse environments.

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🔍 **Schema Validation** | Compare source/target schemas with type coercion rules, nullable detection, and partition column verification |
| 🔄 **DAG Generation** | Auto-generate Airflow DAGs from YAML configs with retries, SLA alerts, and Slack notifications |
| 📦 **Incremental & Full Load** | Support both patterns with watermark-based CDC and partition-level tracking |
| ✅ **Data Integrity** | Row count validation, column-level checksums, and statistical drift detection |
| 💾 **Checkpointing** | Resume interrupted migrations from the last successful partition |
| 📊 **Structured Logging** | JSON-formatted logs with correlation IDs for observability |
| 🏷️ **Multi-Format Support** | Delta Lake, Apache Iceberg, and Parquet targets |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Apache Spark 3.4+ (local or cluster)
- Apache Airflow 2.x (for DAG deployment)

### Installation

```bash
git clone https://github.com/sahulkrishna0-lab/spark-migration-framework.git
cd spark-migration-framework

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Run Your First Migration

```bash
# 1. Validate schema compatibility
python -m src.schema_validator --config configs/sample_pipeline.yaml

# 2. Generate Airflow DAG
python -m src.dag_generator --config configs/sample_pipeline.yaml --output dags/

# 3. Execute migration
python -m src.pipeline_migrator --config configs/sample_pipeline.yaml
```

---

## 📖 Usage Examples

### Schema Validation

```python
from src.schema_validator import SchemaValidator
from src.config import PipelineConfig

config = PipelineConfig.from_yaml("configs/sample_pipeline.yaml")
validator = SchemaValidator(spark_session)

report = validator.validate(
    source_table="legacy_db.customer_events",
    target_table="lakehouse.customer_events",
    config=config
)

if not report.is_compatible:
    print(f"Found {len(report.mismatches)} schema mismatches:")
    for m in report.mismatches:
        print(f"  - {m.column}: {m.source_type} -> {m.target_type} ({m.severity})")
```

### DAG Generation

```python
from src.dag_generator import DagGenerator

generator = DagGenerator()
dag_code = generator.generate(
    config_path="configs/sample_pipeline.yaml",
    output_dir="dags/"
)
# Produces: dags/migrate_customer_events_dag.py
```

### Full Migration

```python
from src.pipeline_migrator import PipelineMigrator

migrator = PipelineMigrator(spark_session, config)
result = migrator.execute()

print(f"Migration: {result.status}")
print(f"Rows migrated: {result.rows_migrated:,}")
print(f"Duration: {result.duration_seconds}s")
```

---

## 📁 Project Structure

```
spark-migration-framework/
├── src/
│   ├── __init__.py
│   ├── schema_validator.py    # Schema comparison engine
│   ├── dag_generator.py       # Airflow DAG code generation
│   ├── pipeline_migrator.py   # Core migration orchestrator
│   └── config.py              # YAML config management
├── configs/
│   └── sample_pipeline.yaml   # Example pipeline configuration
├── tests/
│   ├── test_schema_validator.py
│   └── test_dag_generator.py
├── dags/                      # Generated Airflow DAGs (gitignored)
├── logs/                      # Migration logs (gitignored)
├── requirements.txt
├── Makefile
├── .gitignore
└── README.md
```

---

## 🛠️ Tech Stack

- **Compute**: Apache Spark 3.4 (PySpark)
- **Orchestration**: Apache Airflow 2.x
- **Storage Format**: Delta Lake 2.4 / Apache Iceberg
- **Cloud**: AWS (S3, Glue, EMR) — adaptable to Azure/GCP
- **Testing**: pytest, pyspark.testing
- **Linting**: ruff, mypy

---

## 🧪 Running Tests

```bash
# Run all tests
make test

# Run with coverage
make coverage

# Lint and format
make lint
make format
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Sahul Krishna Karanam** — Cloud Engineer & Solutions Architect

Built from production experience migrating enterprise data platforms to modern lakehouse architectures.
