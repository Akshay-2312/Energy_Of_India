"""
Reproducible local exporter for Energy_Of_India Gold & Audit tables.

Runs smart/transformations/my_transformation.py locally using a lightweight
fake `dlt` module and batch CSV reads on the four source CSVs in the repository
root, enforces all DLT expectations (in PySpark ANSI mode), verifies data
integrity invariants, and writes one flat UTF-8 CSV per Power BI contract table
into smart/powerbi/data/.
"""

from __future__ import annotations

import csv
import datetime
import math
import shutil
import sys
import types
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

POWERBI_DIR = Path(__file__).resolve().parent
REPO_ROOT = POWERBI_DIR.parent
PIPELINE_PATH = REPO_ROOT / "smart" / "transformations" / "my_transformation.py"
OUT_DIR = POWERBI_DIR / "data"

PIPELINE_ORDER = [
    "bronze_daily_energy",
    "bronze_peak_energy",
    "bronze_peak_unmet",
    "bronze_weather",
    "silver_daily_energy_pipeline",
    "silver_peak_energy_pipeline",
    "silver_peak_unmet_pipeline",
    "silver_weather_pipeline",
    "silver_energy_long",
    "silver_missing_date_audit",
    "silver_entity_coverage",
    "gold_daily_energy",
    "gold_peak_energy",
    "gold_peak_unmet",
    "gold_national_daily",
    "gold_weather",
    "gold_state_energy",
    "gold_non_state_entities",
    "gold_dim_state",
    "gold_energy_weather",
    "gold_monthly_summary",
    "gold_state_monthly_summary",
]

EXPORT_SPECS: dict[str, list[str]] = {
    "gold_dim_state": ["State"],
    "gold_energy_weather": ["Date", "State"],
    "gold_weather": ["Date", "State"],
    "gold_national_daily": ["Date"],
    "gold_monthly_summary": ["Year", "Month"],
    "gold_state_monthly_summary": ["State", "Year", "Month"],
    "silver_missing_date_audit": ["Date"],
    "silver_entity_coverage": ["Entity", "Entity_Type"],
    "gold_non_state_entities": ["Date", "Entity"],
}

ALLOWED_REGIONS = {"North", "South", "East", "West", "Central", "North-East", "Islands"}


def format_csv_cell(value: object) -> object:
    """Format Spark row values for Power BI CSV ingestion (ISO dates, true/false)."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, float) and math.isnan(value):
        return ""
    return value


def write_single_csv(df, sort_cols: list[str], dest: Path) -> int:
    """Collect a Spark DataFrame and write a single flat UTF-8 CSV file."""
    if dest.exists() and dest.is_dir():
        shutil.rmtree(dest)

    ordered = df.orderBy(*sort_cols)
    columns = ordered.columns
    rows = ordered.collect()

    with dest.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([format_csv_cell(row[col]) for col in columns])

    return len(rows)


def run_pipeline() -> tuple[SparkSession, dict[str, object]]:
    spark = (
        SparkSession.builder.appName("GridPulse-Export-Gold")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    registry: dict[str, object] = {}
    results: dict[str, object] = {}

    def table(name=None, comment=None, **_):
        def deco(fn):
            registry[name] = fn
            return fn

        return deco

    def _exp(kind: str):
        def maker(label: str, cond: str):
            def deco(fn):
                fn._exp = getattr(fn, "_exp", []) + [(kind, label, cond)]
                return fn

            return deco

        return maker

    def read(name: str):
        return results[name]

    fake_dlt = types.ModuleType("dlt")
    fake_dlt.table = table
    fake_dlt.read = read
    fake_dlt.expect = _exp("warn")
    fake_dlt.expect_or_drop = _exp("drop")
    sys.modules["dlt"] = fake_dlt

    src = PIPELINE_PATH.read_text(encoding="utf-8")
    ns = {"spark": spark, "__name__": "pipeline"}
    exec(compile(src, str(PIPELINE_PATH), "exec"), ns)

    def batch_read_raw_csv(_path: str, file_name: str):
        local_csv = str(REPO_ROOT / file_name)
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(local_csv)
        df = df.toDF(*[ns["sanitize_column_name"](c) for c in df.columns])
        return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
            "_source_file", F.lit(local_csv)
        )

    ns["read_raw_csv"] = batch_read_raw_csv

    assert set(PIPELINE_ORDER) == set(registry), (
        f"Pipeline table mismatch: {set(PIPELINE_ORDER) ^ set(registry)}"
    )

    for name in PIPELINE_ORDER:
        fn = registry[name]
        df = fn()
        for kind, label, cond in getattr(fn, "_exp", []):
            bad = df.filter(~F.expr(f"coalesce({cond}, false)")).count()
            if kind == "drop":
                df = df.filter(F.expr(f"coalesce({cond}, false)"))
            print(f"  [{kind:4}] {name}.{label}: violations={bad}")
        results[name] = df.cache()
        print(f"{name:32} rows={results[name].count():>7} cols={len(df.columns)}")

    return spark, results


def verify_tables(results: dict[str, object]) -> None:
    print("\n--- VERIFICATION ---")
    for table_name, keys in EXPORT_SPECS.items():
        df = results[table_name]
        total_rows = df.count()
        for key in keys:
            null_count = df.filter(F.col(key).isNull()).count()
            assert null_count == 0, f"{table_name}.{key} has {null_count} NULL values"
        dup_count = total_rows - df.dropDuplicates(keys).count()
        assert dup_count == 0, f"{table_name} has {dup_count} duplicate rows on {keys}"
        print(f"  OK {table_name}: rows={total_rows}, dup_keys({keys})=0, null_keys=0")

    # Additional Month_Start uniqueness check on monthly summaries
    for m_table, m_keys in [
        ("gold_monthly_summary", ["Month_Start"]),
        ("gold_state_monthly_summary", ["State", "Month_Start"]),
    ]:
        df = results[m_table]
        assert df.filter(F.col("Month_Start").isNull()).count() == 0
        assert df.count() == df.dropDuplicates(m_keys).count()

    # dim_state checks
    dim_state = results["gold_dim_state"]
    state_count = dim_state.count()
    assert state_count == 36, f"Expected 36 states in gold_dim_state, got {state_count}"
    bad_regions = dim_state.filter(
        F.col("Region").isNull() | ~F.col("Region").isin(list(ALLOWED_REGIONS))
    ).count()
    assert bad_regions == 0, f"Invalid or NULL regions in gold_dim_state: {bad_regions}"
    assert dim_state.filter(F.col("City").isNull()).count() == 0, "NULL City in gold_dim_state"
    assert dim_state.filter(F.col("Has_Weather")).count() == 36
    assert dim_state.filter(F.col("Has_Energy")).count() == 33

    # Latitude / Longitude moved out of gold_weather & gold_energy_weather
    for fact_name in ("gold_weather", "gold_energy_weather"):
        cols = results[fact_name].columns
        assert "Latitude" not in cols and "Longitude" not in cols, (
            f"Coordinates still present in {fact_name}: {cols}"
        )

    # Referential integrity: every State in fact tables exists in gold_dim_state
    dim_states = dim_state.select("State").distinct()
    for fact_name in ("gold_energy_weather", "gold_weather", "gold_state_monthly_summary"):
        orphans = results[fact_name].select("State").distinct().join(dim_states, "State", "left_anti").count()
        assert orphans == 0, f"{fact_name} has {orphans} states missing from gold_dim_state"
        print(f"  OK {fact_name} -> gold_dim_state referential integrity (0 orphans)")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    spark, results = run_pipeline()
    try:
        verify_tables(results)
        print("\n--- EXPORTING CSVs ---")
        for table_name, sort_cols in EXPORT_SPECS.items():
            dest = OUT_DIR / f"{table_name}.csv"
            df = results[table_name]
            written_rows = write_single_csv(df, sort_cols, dest)
            assert dest.is_file(), f"Missing exported file: {dest}"
            print(f"  Wrote {dest.name:34} rows={written_rows:>7} cols={df.columns}")
    finally:
        spark.stop()

    print("\nALL CHECKS & EXPORTS PASSED")


if __name__ == "__main__":
    main()
