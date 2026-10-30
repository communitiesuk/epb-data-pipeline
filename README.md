# EPB Data Pipeline

Pipeline for the Energy Performance of Buildings (EPB) Data Platform.

This repository organizes, tests, and deploys AWS Glue ETL jobs and PySpark scripts responsible for synchronizing EPC (Energy Performance Certificate) data from PostgreSQL into Apache Iceberg tables in the AWS Glue Data Catalog, as well as generating annual data exports.

---

## Architecture Overview

- **`src/jobs/`**: Production AWS Glue jobs executed in AWS:
  - **`populate_iceberg_catalog.py`**: Extracts data from PostgreSQL, dynamically synchronizes catalog schemas, executes Iceberg `MERGE INTO` operations, and coordinates AWS Glue table optimizers (compaction, snapshot retention, and orphan file deletion).
  - **`export_by_year.py`**: Reads EPC datasets from the Glue Catalog and generates year-partitioned CSV zip archives in S3.
  - **`export_json_by_year.py`**: Exports year-partitioned EPC datasets in JSON format.
  - **`create_data_catalog.py`**: Manages Glue Catalog table creation and blue/green S3 location switching.
  - **`insert_data.py` / `delete_data.py`**: Utilities for inserting and managing data lifecycle.
- **`src/scripts/`**: Modularized PySpark logic (e.g. `populate_iceberg.py`) decoupled from AWS Glue runtime dependencies to enable fast local unit testing.
- **`tests/`**: Unit and integration test suite running local Spark sessions with Iceberg Hadoop catalogs.
- **`buildspec/`**: AWS CodeBuild configurations for automated CI testing and S3 script deployment.

---

## Prerequisites

- **Python**: `==3.11.*`
- **[uv](https://docs.astral.sh/uv/)**: `>=0.13.6` (Python package and virtual environment manager)
- **Java**: **Java 17 (OpenJDK 17)** or Java 8/11 (PySpark 3.5.x is not compatible with Java 21+)
- **AWS CLI**: Required for manual deployments or assuming AWS roles

---

## Local Setup

### 1. Clone the repository

```bash
git clone git@github.com:communitiesuk/epb-data-pipeline.git
cd epb-data-pipeline
```

### 2. Install dependencies

Sync the virtual environment and development dependencies using `uv`:

```bash
uv sync
```

### 3. Configure Local Environment (`.env`)

PySpark requires a compatible Java runtime (Java 17). Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Ensure `JAVA_HOME` in `.env` points to your Java 17 installation. For macOS with Homebrew:

```env
JAVA_HOME=/opt/homebrew/Cellar/openjdk@17/17.0.21.0.2/libexec/openjdk.jdk/Contents/Home
```

*(You can verify your local Java 17 path by running `/usr/libexec/java_home -v 17`)*.

---

## Running Tests & Code Quality

### Running Unit Tests

Unit tests spin up an in-memory local Spark session configured with the Apache Iceberg Spark runtime:

```bash
uv run pytest
```

To run a specific test file:

```bash
uv run pytest tests/test_iceberg_populate.py
```

### Code Formatting & Linting

Run Ruff to inspect and format code:

```bash
uv run ruff check .
uv run ruff format --check .
```

### Dependency Audit

Audit installed packages for known vulnerabilities:

```bash
uv run pip-audit
```

---

## CI/CD Pipeline

The project uses AWS CodeBuild pipelines defined in `buildspec/`:

1. **Test Phase (`buildspec/run_tests.yml`)**:
   - Installs `uv` and synchronizes project dependencies.
   - Runs `uv run pytest tests/`.
2. **Deploy Phase (`buildspec/deploy_to_s3.yml`)**:
   - Assumes the deployment role in the target account via `scripts/assume_role.sh`.
   - Synchronizes Python scripts in `src/jobs/` to the target S3 bucket for AWS Glue execution.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
