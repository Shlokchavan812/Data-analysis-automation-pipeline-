# Student Performance Data Analysis Pipeline Automation (Apache Airflow)

This project automates the data analysis pipeline from `Analysis.ipynb` using **Apache Airflow**.

---

## 📅 Schedule & Cron Configuration

The DAG is scheduled to run **every day at 12:00 PM**:
- **Cron Expression**: `0 12 * * *`
- **Schedule**: Daily at 12:00 PM (configured for your timezone in `dags/student_analysis_dag.py`)
- **Catchup**: `False` (avoids running historical backfills on startup)

---

## 🏗 Pipeline Architecture & Task Flow

```
start_pipeline
      │
      ▼
check_source_data  (Verifies student_performance_clean_reference (1).csv exists)
      │
      ▼
   clean_data      (Deduplication, numeric casting, outlier removal, median imputation)
      │
      ▼
 transform_data    (Binned feature engineering: grade brackets & attendance groups)
      │
      ▼
analyze_and_export (Grouped stats by attendance group & correlation matrix)
      │
      ▼
pipeline_completed
```

### Generated Output Files
- `clean_students.csv`: Cleaned dataset.
- `transformed_students.csv`: Cleaned dataset with `grade` and `att_group` features.
- `analysis_summary.csv`: Aggregated metrics (study hours, attendance, student count).
- `analysis_correlation.csv`: Correlation matrix across numerical variables.

---

## 🚀 How to Run the Airflow Pipeline

### Option 1: Quick Start with Docker (Recommended)

1. Make sure **Docker Desktop** is running.
2. From the project directory, run:
   ```bash
   docker compose up -d
   ```
3. Open your browser and navigate to:
   - **URL**: [http://localhost:8080](http://localhost:8080)
   - Credentials will be displayed in `standalone_admin_password.txt` or default to `admin` / `admin`.
4. Locate the DAG **`student_data_analysis_pipeline`**, ensure it is toggled **ON**, and inspect the daily 12:00 PM schedule.

To shut down:
```bash
docker compose down
```

---

### Option 2: Running via Local Python / WSL / Linux

1. Set up a virtual environment:
   ```bash
   python -m venv airflow_env
   # On Linux/WSL:
   source airflow_env/bin/activate
   # On Windows PowerShell (if running standalone):
   .\airflow_env\Scripts\Activate.ps1
   ```

2. Install dependencies:
   ```bash
   pip install apache-airflow pandas numpy pendulum
   ```

3. Set the Airflow DAG directory and start Airflow:
   ```bash
   export AIRFLOW_HOME="$(pwd)"
   airflow standalone
   ```

---

### Option 3: Direct Pipeline Execution (Without Airflow)

To verify the ETL logic directly at any time:
```bash
python pipeline.py
```

