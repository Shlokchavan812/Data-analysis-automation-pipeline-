"""
Data Analysis Pipeline for Student Performance Dataset.
Contains modular functions for Extract, Clean, Transform, Analyze, and Load (ETL).
"""

import os
import logging
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT_FILE = os.path.join(BASE_DIR, "student_performance_clean_reference (1).csv")
CLEAN_OUTPUT_FILE = os.path.join(BASE_DIR, "clean_students.csv")
TRANSFORMED_OUTPUT_FILE = os.path.join(BASE_DIR, "transformed_students.csv")
ANALYSIS_SUMMARY_FILE = os.path.join(BASE_DIR, "analysis_summary.csv")
ANALYSIS_CORR_FILE = os.path.join(BASE_DIR, "analysis_correlation.csv")


def clean_data(input_path: str = DEFAULT_INPUT_FILE, output_path: str = CLEAN_OUTPUT_FILE) -> pd.DataFrame:
    """Extract raw data, drop duplicates, convert numeric columns, filter outliers, and impute missing values."""
    logger.info(f"Reading raw data from: {input_path}")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found at {input_path}")

    df = pd.read_csv(input_path)
    logger.info(f"Initial shape: {df.shape}")

    # Remove duplicates
    df = df.drop_duplicates()

    # Numeric conversion
    cols = ["study_hours_per_week", "attendance_pct", "previous_marks"]
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Filter attendance and marks within valid bounds
    if "attendance_pct" in df.columns and "final_marks" in df.columns:
        df = df[(df["attendance_pct"] <= 100) & (df["final_marks"] <= 100)]

    # Impute numeric missing values with median
    numeric_medians = df.median(numeric_only=True)
    df = df.fillna(numeric_medians)

    logger.info(f"Cleaned shape: {df.shape}")
    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved cleaned data to: {output_path}")

    return df


def transform_data(input_path: str = CLEAN_OUTPUT_FILE, output_path: str = TRANSFORMED_OUTPUT_FILE) -> pd.DataFrame:
    """Apply feature engineering and binning (grade categories and attendance groups)."""
    logger.info(f"Loading data for transformation from: {input_path}")
    df = pd.read_csv(input_path)

    # Grade categorization
    if "final_marks" in df.columns:
        df["grade"] = pd.cut(
            df["final_marks"],
            bins=[0, 49, 79, 100],
            labels=["Needs Improvement", "Average", "Excellent"],
            include_lowest=True
        )

    # Attendance categorization
    if "attendance_pct" in df.columns:
        df["att_group"] = pd.cut(
            df["attendance_pct"],
            bins=[0, 75, 90, 100],
            labels=["Low", "Medium", "High"],
            include_lowest=True
        )

    logger.info(f"Transformed features added. Shape: {df.shape}")
    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved transformed data to: {output_path}")

    return df


def analyse_and_export(
    input_path: str = TRANSFORMED_OUTPUT_FILE,
    summary_path: str = ANALYSIS_SUMMARY_FILE,
    corr_path: str = ANALYSIS_CORR_FILE
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute summary statistics grouped by attendance and compute correlation matrix."""
    logger.info(f"Analyzing data from: {input_path}")
    df = pd.read_csv(input_path)

    if "att_group" in df.columns:
        summary = df.groupby("att_group", observed=False).agg(
            avg_study=("study_hours_per_week", "mean"),
            avg_attendance=("attendance_pct", "mean"),
            Students=("student_id", "count")
        )
    else:
        summary = df.describe()

    corr = df.corr(numeric_only=True)

    if summary_path:
        os.makedirs(os.path.dirname(os.path.abspath(summary_path)), exist_ok=True)
        summary.to_csv(summary_path, index=True)
        logger.info(f"Saved summary statistics to: {summary_path}")

    if corr_path:
        os.makedirs(os.path.dirname(os.path.abspath(corr_path)), exist_ok=True)
        corr.to_csv(corr_path, index=True)
        logger.info(f"Saved correlation matrix to: {corr_path}")

    return summary, corr


def run_full_pipeline():
    """Execute the end-to-end pipeline."""
    logger.info("Starting pipeline execution...")
    clean_data()
    transform_data()
    summary, corr = analyse_and_export()
    logger.info("Pipeline completed successfully!")
    print("\n--- Summary Statistics ---")
    print(summary)
    print("\n--- Correlation Head ---")
    print(corr.iloc[:5, :5])


if __name__ == "__main__":
    run_full_pipeline()

