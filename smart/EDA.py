# Databricks notebook source
print("========== ZENODO DATASETS ==========")

display(
    dbutils.fs.ls(
        "/Volumes/workspace/default/smartgrid_raw/zenodo/"
    )
)

print("========== OPEN METEO DATASET ==========")

display(
    dbutils.fs.ls(
        "/Volumes/workspace/default/smartgrid_raw/open_meteo/"
    )
)

# COMMAND ----------

zenodo_path = "/Volumes/workspace/default/smartgrid_raw/zenodo/"

open_meteo_path = "/Volumes/workspace/default/smartgrid_raw/open_meteo/"

# COMMAND ----------


# ============================================================
# ENERGY BIG DATA PROJECT - COMPLETE EDA AND SILVER NOTEBOOK
# Apache Spark | Databricks | Python | Delta Tables
# ============================================================

from pyspark.sql import functions as F
import re

print("=" * 65)
print("ENERGY BIG DATA PROJECT - EDA STARTED")
print("=" * 65)


# ============================================================
# 1. DATA SOURCE PATHS
# ============================================================

base = "/Volumes/workspace/default/smartgrid_raw/"

print("\n1. CHECKING SOURCE FILES")

print("Zenodo files:")
print(dbutils.fs.ls(base + "zenodo/"))

print("Open Meteo files:")
print(dbutils.fs.ls(base + "open_meteo/"))


# ============================================================
# 2. LOAD ALL FOUR DATASETS
# ============================================================

print("\n2. LOADING DATASETS")

df_daily_energy = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(base + "zenodo/corrected_daily_energy_met_MU.csv")
)

df_peak_energy = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(base + "zenodo/corrected_peak_met_MW.csv")
)

df_peak_unmet = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(base + "zenodo/peak_unmet_MW.csv")
)

df_weather = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(base + "open_meteo/all_india_weather.csv")
)

datasets = {
    "Daily Energy": df_daily_energy,
    "Peak Energy": df_peak_energy,
    "Peak Unmet": df_peak_unmet,
    "Open Meteo Weather": df_weather
}

print("SUCCESS! All four datasets loaded.")


# ============================================================
# 3. DATASET PREVIEW AND SCHEMA
# ============================================================

print("\n3. DATASET PREVIEW AND SCHEMA")

for name, df in datasets.items():

    print(f"\n========== {name} ==========")

    print("Rows:", df.count())
    print("Columns:", len(df.columns))

    df.printSchema()
    display(df.limit(5))


# ============================================================
# 4. MISSING VALUE ANALYSIS
# ============================================================

print("\n4. MISSING VALUE ANALYSIS")

for name, df in datasets.items():

    print(f"\n========== {name} ==========")

    expressions = []

    for column_name, data_type in df.dtypes:

        condition = F.col(column_name).isNull()

        if data_type == "string":
            condition = condition | (F.trim(F.col(column_name)) == "")

        expressions.append(
            F.sum(
                F.when(condition, 1).otherwise(0)
            ).alias(column_name)
        )

    results = df.agg(*expressions).first().asDict()

    found_missing = False

    for column_name, value in results.items():

        if value and value > 0:

            print(f"{column_name}: {value} missing values")
            found_missing = True

    if not found_missing:
        print("No missing values found!")


# ============================================================
# 5. DUPLICATE RECORD ANALYSIS
# ============================================================

print("\n5. DUPLICATE RECORD ANALYSIS")

for name, df in datasets.items():

    total_rows = df.count()
    unique_rows = df.dropDuplicates().count()

    print(f"\n========== {name} ==========")
    print("Total rows:", total_rows)
    print("Unique rows:", unique_rows)
    print("Duplicate rows:", total_rows - unique_rows)


# ============================================================
# 6. DATE RANGE AND EMPTY COLUMN ANALYSIS
# ============================================================

print("\n6. DATE RANGE AND EMPTY COLUMN ANALYSIS")

for name, df in datasets.items():

    print(f"\n========== {name} ==========")

    df.select(
        F.min("Date").alias("First Date"),
        F.max("Date").alias("Last Date")
    ).show()

    empty_columns = []

    for column_name in df.columns:

        if (
            df.filter(F.col(column_name).isNotNull())
            .limit(1)
            .count() == 0
        ):
            empty_columns.append(column_name)

    print("Completely empty columns:")
    print(empty_columns if empty_columns else "None")


# ============================================================
# 7. REPEATED DATE ANALYSIS
# ============================================================

print("\n7. REPEATED DATE ANALYSIS")

for name, df in datasets.items():

    print(f"\n========== {name} ==========")

    duplicate_dates = (
        df.groupBy("Date")
        .count()
        .filter(F.col("count") > 1)
        .orderBy(F.col("count").desc())
    )

    print("Number of dates appearing more than once:")
    print(duplicate_dates.count())

    display(duplicate_dates.limit(10))


# ============================================================
# 8. INSPECT SAMPLE REPEATED DATE
# ============================================================

print("\n8. SAMPLE DATE INSPECTION")

check_date = "2016-06-15"

for name, df in datasets.items():

    if name == "Open Meteo Weather":
        continue

    print(f"\n========== {name} ==========")

    display(
        df.filter(F.col("Date") == check_date)
    )


# ============================================================
# 9. CREATE CLEANED SILVER DATAFRAMES
# ============================================================

print("\n9. SILVER DATA CLEANING")

# Completely empty columns in energy datasets
empty_columns = [
    "Andaman and Nicobar Islands",
    "Lakshadweep"
]

# Daily Energy
silver_daily_energy = (
    df_daily_energy
    .drop(*empty_columns)
    .dropDuplicates()
)

# Peak Energy
silver_peak_energy = (
    df_peak_energy
    .drop(*empty_columns)
    .dropDuplicates()
)

# Peak Unmet
silver_peak_unmet = (
    df_peak_unmet
    .drop(*empty_columns)
    .dropDuplicates()
)

# Weather
silver_weather = df_weather.dropDuplicates()

silver_datasets = {
    "Silver Daily Energy": silver_daily_energy,
    "Silver Peak Energy": silver_peak_energy,
    "Silver Peak Unmet": silver_peak_unmet,
    "Silver Weather": silver_weather
}

for name, df in silver_datasets.items():

    print(f"\n{name}")
    print("Rows:", df.count())
    print("Columns:", len(df.columns))

print("\nSilver cleaning completed!")


# ============================================================
# 10. MISSING VALUE ANALYSIS AFTER CLEANING
# ============================================================

print("\n10. SILVER MISSING VALUE ANALYSIS")

for name, df in silver_datasets.items():

    print(f"\n========== {name} ==========")

    expressions = [
        F.sum(
            F.when(F.col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in df.columns
    ]

    results = df.agg(*expressions).first().asDict()

    found_missing = False

    for column_name, count in results.items():

        if count and count > 0:

            print(f"{column_name}: {count} missing values")
            found_missing = True

    if not found_missing:
        print("No NULL values found!")


# ============================================================
# 11. INSPECT MISSING DATES IN ENERGY DATASETS
# ============================================================

print("\n11. INSPECT MISSING DATES")

columns_to_check = [
    "Chhattisgarh",
    "Dadra and Nagar Haveli",
    "Daman and Diu",
    "Telangana",
    "AMNSIL",
    "Andhra Pradesh",
    "Karnataka",
    "Kerala",
    "Puducherry",
    "Tamil Nadu",
    "Total"
]

energy_silver_datasets = {
    "Silver Daily Energy": silver_daily_energy,
    "Silver Peak Energy": silver_peak_energy,
    "Silver Peak Unmet": silver_peak_unmet
}

for name, df in energy_silver_datasets.items():

    print(f"\n========== {name} ==========")

    for column_name in columns_to_check:

        if column_name in df.columns:

            missing = df.filter(F.col(column_name).isNull())

            if missing.limit(1).count() > 0:

                print(f"\n{column_name} - missing dates:")

                display(
                    missing.select("Date")
                    .orderBy("Date")
                )


# ============================================================
# 12. CHECK MISSING DATE RANGES
# ============================================================

print("\n12. MISSING DATE RANGE ANALYSIS")

columns_to_check = [
    "Dadra and Nagar Haveli",
    "Daman and Diu",
    "Telangana",
    "AMNSIL"
]

for name, df in energy_silver_datasets.items():

    print(f"\n========== {name} ==========")

    for column_name in columns_to_check:

        if column_name in df.columns:

            print(f"\n{column_name}")

            df.filter(
                F.col(column_name).isNull()
            ).agg(
                F.min("Date").alias("First Missing Date"),
                F.max("Date").alias("Last Missing Date"),
                F.count("*").alias("Missing Rows")
            ).show()


# ============================================================
# 13. VERIFY SOURCE TOTAL COLUMN
# ============================================================

print("\n13. SOURCE TOTAL COLUMN VERIFICATION")

check_date = "2023-12-05"

for name, df in energy_silver_datasets.items():

    print(f"\n========== {name} ==========")

    display(
        df.filter(F.col("Date") == check_date)
        .select(
            "Date",
            "Telangana",
            "AMNSIL",
            "Damodar Valley Corporation",
            "Total"
        )
    )


# ============================================================
# 14. CLEAN COLUMN NAMES FOR DELTA COMPATIBILITY
# ============================================================

print("\n14. CLEANING COLUMN NAMES")

def clean_column_names(df):

    new_names = []

    for old_name in df.columns:

        new_name = re.sub(
            r'[ ,;{}()\n\t=]+',
            '_',
            old_name
        )

        new_name = new_name.strip('_')

        new_names.append(new_name)

    # Check for duplicate column names after cleaning
    if len(new_names) != len(set(new_names)):

        raise ValueError(
            "Duplicate column names found after cleaning!"
        )

    return df.toDF(*new_names)


# Create separate EDA DataFrames
eda_silver_daily_energy = clean_column_names(silver_daily_energy)

eda_silver_peak_energy = clean_column_names(silver_peak_energy)

eda_silver_peak_unmet = clean_column_names(silver_peak_unmet)

eda_silver_weather = clean_column_names(silver_weather)

print("All column names cleaned successfully!")


# ============================================================
# 15. PREVIEW CLEANED COLUMN NAMES
# ============================================================

print("\n15. CLEANED COLUMN NAME PREVIEW")

for name, df in {
    "EDA Silver Daily Energy": eda_silver_daily_energy,
    "EDA Silver Peak Energy": eda_silver_peak_energy,
    "EDA Silver Peak Unmet": eda_silver_peak_unmet,
    "EDA Silver Weather": eda_silver_weather
}.items():

    print(f"\n========== {name} ==========")
    print(df.columns)


# ============================================================
# 16. CHECK CURRENT CATALOG AND SCHEMA
# ============================================================

print("\n16. CURRENT CATALOG AND SCHEMA")

spark.sql(
    "SELECT current_catalog(), current_schema()"
).show(truncate=False)


# ============================================================
# 17. SAVE FOUR SEPARATE EDA SILVER DELTA TABLES
# ============================================================


# ============================================================
# 17. SAVE FOUR EDA SILVER DELTA TABLES
# ============================================================

print("\nSAVING EDA SILVER TABLES")

eda_tables = {
    "eda_silver_daily_energy": eda_silver_daily_energy,
    "eda_silver_peak_energy": eda_silver_peak_energy,
    "eda_silver_peak_unmet": eda_silver_peak_unmet,
    "eda_silver_weather": eda_silver_weather
}

for table_name, df in eda_tables.items():

    full_table_name = f"workspace.default.{table_name}"

    if spark.catalog.tableExists(full_table_name):

        print(f"TABLE ALREADY EXISTS - SKIPPED: {full_table_name}")

    else:

        (
            df.write
            .format("delta")
            .mode("append")
            .saveAsTable(full_table_name)
        )

        print(f"SUCCESSFULLY SAVED: {full_table_name}")

print("\nTABLE SAVING PROCESS COMPLETED!")



# ============================================================
# 18. VERIFY ALL SAVED TABLES
# ============================================================

print("\n18. VERIFYING SAVED TABLES")

display(
    spark.sql("SHOW TABLES IN workspace.default")
)

for table_name in eda_tables.keys():

    full_table_name = f"workspace.default.{table_name}"

    if spark.catalog.tableExists(full_table_name):

        saved_df = spark.table(full_table_name)

        print(f"\nFOUND: {full_table_name}")
        print("Rows:", saved_df.count())
        print("Columns:", len(saved_df.columns))

    else:

        print(f"\nNOT FOUND: {full_table_name}")


# ============================================================
# 19. PREVIEW ALL FOUR SAVED TABLES
# ============================================================

print("\n19. SAVED TABLE PREVIEW")

for table_name in eda_tables.keys():

    full_table_name = f"workspace.default.{table_name}"

    if spark.catalog.tableExists(full_table_name):

        print(f"\n========== {table_name} ==========")

        display(
            spark.table(full_table_name).limit(5)
        )


# ============================================================
# 20. FINAL STATUS
# ============================================================

print("\n" + "=" * 65)
print("ENERGY BIG DATA EDA PROCESS COMPLETED")
print("=" * 65)

print("Raw datasets loaded: 4")
print("Silver DataFrames cleaned: 4")
print("EDA Silver tables checked: 4")

print("\nNext stage:")
print("GOLD LAYER TRANSFORMATIONS AND POWER BI DASHBOARD")

print("=" * 65)

# Check whether each EDA table exists and display its row count

eda_table_names = [
    "eda_silver_daily_energy",
    "eda_silver_peak_energy",
    "eda_silver_peak_unmet",
    "eda_silver_weather"
]

for table_name in eda_table_names:

    full_name = f"workspace.default.{table_name}"

    if spark.catalog.tableExists(full_name):
        df = spark.table(full_name)

        print(f"FOUND: {full_name}")
        print(f"Rows: {df.count()}")
        print(f"Columns: {len(df.columns)}")

        display(df.limit(5))

    else:
        print(f"NOT FOUND: {full_name}")


# COMMAND ----------


silver_tables = [
    "silver_daily_energy_pipeline",
    "silver_peak_energy_pipeline",
    "silver_peak_unmet_pipeline",
    "silver_weather_pipeline"
]

for table_name in silver_tables:
    full_name = f"workspace.default.{table_name}"
    
    print("\n==========", table_name, "==========")
    df = spark.table(full_name)
    
    print("Rows:", df.count())
    print("Columns:", df.columns)
    df.printSchema()


# COMMAND ----------

for table in [
    "gold_daily_energy",
    "gold_peak_energy",
    "gold_peak_unmet",
    "gold_weather"
]:
    print(table, spark.catalog.tableExists(f"workspace.default.{table}"))

# COMMAND ----------


gold_tables = [
    "gold_daily_energy",
    "gold_peak_energy",
    "gold_peak_unmet",
    "gold_weather"
]

for table in gold_tables:
    full_table_name = f"workspace.default.{table}"

    print(f"\n========== {table} ==========")

    df = spark.table(full_table_name)

    print("Total Rows:", df.count())
    print("Columns:", df.columns)

    display(df.limit(10))


# COMMAND ----------

for table in gold_tables:
    print(f"\n===== {table} =====")
    spark.table(f"workspace.default.{table}").printSchema()

# COMMAND ----------

df = spark.table("workspace.default.gold_state_energy")

print("Total Rows:", df.count())

display(df.limit(10))

# COMMAND ----------

df = spark.table("workspace.default.gold_monthly_summary")

print("Total Rows:", df.count())

display(df.orderBy("Year", "Month").limit(12))

# COMMAND ----------

df = spark.table("workspace.default.gold_energy_weather")

print("Total Rows:", df.count())

display(df.limit(10))

# COMMAND ----------

gold_tables = [
    "gold_daily_energy",
    "gold_peak_energy",
    "gold_peak_unmet",
    "gold_weather",
    "gold_state_energy",
    "gold_monthly_summary",
    "gold_energy_weather"
]

for table in gold_tables:
    df = spark.table(f"workspace.default.{table}")

    print("\nTable:", table)
    print("Rows:", df.count())
    print("Columns:", len(df.columns))