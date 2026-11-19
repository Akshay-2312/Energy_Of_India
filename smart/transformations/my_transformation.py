"""
GridPulse India - Medallion Lakehouse (Lakeflow Declarative Pipeline)

    Unity Catalog Volume (raw CSV)
        -> BRONZE  (4 Auto Loader streams, one per source file, raw strings)
        -> SILVER  (typed, de-duplicated by Date key, empty regions dropped,
                    states separated from utility / national-total entities)
        -> GOLD    (state-grain energy + weather, monthly summaries for Power BI)

Volume layout expected:
    /Volumes/workspace/default/smartgrid_raw/zenodo/corrected_daily_energy_met_MU.csv
    /Volumes/workspace/default/smartgrid_raw/zenodo/corrected_peak_met_MW.csv
    /Volumes/workspace/default/smartgrid_raw/zenodo/peak_unmet_MW.csv
    /Volumes/workspace/default/smartgrid_raw/open_meteo/all_india_weather.csv

Design notes
- Bronze keeps every column as a string (inferColumnTypes=false). Type casting
  happens in Silver with try_cast, so one bad cell becomes NULL instead of
  failing the stream or being diverted into _rescued_data.
- Auto Loader schema/checkpoint locations are managed by the pipeline itself;
  setting cloudFiles.schemaLocation inside a declarative pipeline is not
  supported, so it is intentionally not set here.
- Repeated Date rows: 43/43/47 are exact duplicates, 12/12/8 are conflicting
  values for the same date. Silver collapses to one row per Date using the
  per-column MAX (deterministic, ignores NULLs). Change DEDUP_AGG to alter it.
"""

import re

import dlt
from pyspark.sql import functions as F

# ============================================================
# CONFIG
# ============================================================

VOLUME_ROOT = "/Volumes/workspace/default/smartgrid_raw"
ZENODO_PATH = f"{VOLUME_ROOT}/zenodo/"
OPEN_METEO_PATH = f"{VOLUME_ROOT}/open_meteo/"

DAILY_ENERGY_FILE = "corrected_daily_energy_met_MU.csv"
PEAK_MET_FILE = "corrected_peak_met_MW.csv"
PEAK_UNMET_FILE = "peak_unmet_MW.csv"
WEATHER_FILE = "all_india_weather.csv"

# Columns with no data in any row of the three energy files.
EMPTY_REGIONS = ["Andaman_and_Nicobar_Islands", "Lakshadweep"]

# Non-state entities that live in the same wide tables as the states.
UTILITY_ENTITIES = ["AMNSIL", "Damodar Valley Corporation"]
NATIONAL_TOTAL = "Total"

# Energy files still report the two UTs separately, weather uses the merged UT.
MERGED_UT = "Dadra and Nagar Haveli and Daman and Diu"
UT_NAME_MAP = {
    "Dadra and Nagar Haveli": MERGED_UT,
    "Daman and Diu": MERGED_UT,
}

BRONZE_META_COLS = ["_rescued_data", "_ingested_at", "_source_file"]
WEATHER_NUMERIC_COLS = [
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "Latitude",
    "Longitude",
]

ENTITY_TYPE_STATE = "State/UT"
ENTITY_TYPE_UTILITY = "Utility/Industrial"
ENTITY_TYPE_NATIONAL = "National Total"

# Custom analytical Region mapping for all 36 Indian states and UTs in
# gold_dim_state. Note: this grouping is our own analytical taxonomy defined
# for GridPulse regional slicing (not present in the Zenodo or Open-Meteo
# source CSVs). Allowed values: North, South, East, West, Central, North-East, Islands.
STATE_REGION_MAP = {
    # North (9)
    "Chandigarh": "North",
    "Delhi": "North",
    "Haryana": "North",
    "Himachal Pradesh": "North",
    "Jammu and Kashmir": "North",
    "Ladakh": "North",
    "Punjab": "North",
    "Uttar Pradesh": "North",
    "Uttarakhand": "North",
    # South (6)
    "Andhra Pradesh": "South",
    "Karnataka": "South",
    "Kerala": "South",
    "Puducherry": "South",
    "Tamil Nadu": "South",
    "Telangana": "South",
    # East (4)
    "Bihar": "East",
    "Jharkhand": "East",
    "Odisha": "East",
    "West Bengal": "East",
    # West (5)
    "Dadra and Nagar Haveli and Daman and Diu": "West",
    "Goa": "West",
    "Gujarat": "West",
    "Maharashtra": "West",
    "Rajasthan": "West",
    # Central (2)
    "Chhattisgarh": "Central",
    "Madhya Pradesh": "Central",
    # North-East (8)
    "Arunachal Pradesh": "North-East",
    "Assam": "North-East",
    "Manipur": "North-East",
    "Meghalaya": "North-East",
    "Mizoram": "North-East",
    "Nagaland": "North-East",
    "Sikkim": "North-East",
    "Tripura": "North-East",
    # Islands (2)
    "Andaman and Nicobar Islands": "Islands",
    "Lakshadweep": "Islands",
}


# ============================================================
# HELPERS
# ============================================================

def sanitize_column_name(name):
    """Make a header Delta-safe: spaces and ,;{}()\\n\\t= become '_'."""
    if name.startswith("_"):  # Auto Loader metadata such as _rescued_data
        return name
    return re.sub(r"[ ,;{}()\n\t=]+", "_", name).strip("_")


def read_raw_csv(path, file_name):
    """Auto Loader stream for exactly one source CSV, headers sanitized."""
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("pathGlobFilter", file_name)
        .load(path)
    )
    df = df.toDF(*[sanitize_column_name(c) for c in df.columns])
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def clean_energy_wide(bronze_table):
    """
    Wide energy table -> typed, empty regions dropped, one row per Date.
    NULL dates are left in place so the pipeline expectation can drop and count
    them.
    """
    df = dlt.read(bronze_table).drop(*BRONZE_META_COLS).drop(*EMPTY_REGIONS)
    value_cols = [c for c in df.columns if c != "Date"]

    typed = df.selectExpr(
        "try_cast(`Date` as date) as `Date`",
        *[f"try_cast(`{c}` as double) as `{c}`" for c in value_cols],
    )
    return typed.groupBy("Date").agg(*[F.max(c).alias(c) for c in value_cols])


def unpivot_entities(df, value_name):
    """Wide (one column per entity) -> long (Date, Entity, value)."""
    entities = [c for c in df.columns if c != "Date"]
    pairs = []
    for c in entities:
        label = c.replace("_", " ")
        pairs.append(f"'{label}', `{c}`")
    stack_sql = f"stack({len(entities)}, {', '.join(pairs)}) as (Entity, {value_name})"
    return df.selectExpr("`Date`", stack_sql)


def add_calendar_columns(df, quarter=False):
    df = df.withColumn("Year", F.year("Date")).withColumn("Month", F.month("Date"))
    if quarter:
        df = df.withColumn("Quarter", F.quarter("Date"))
    return df


# ============================================================
# BRONZE - raw ingestion, one stream per source file
# ============================================================

@dlt.table(
    name="bronze_daily_energy",
    comment="Raw daily energy met (MU), all columns as strings, headers sanitized.",
)
def bronze_daily_energy():
    return read_raw_csv(ZENODO_PATH, DAILY_ENERGY_FILE)


@dlt.table(
    name="bronze_peak_energy",
    comment="Raw peak power met (MW), all columns as strings, headers sanitized.",
)
def bronze_peak_energy():
    return read_raw_csv(ZENODO_PATH, PEAK_MET_FILE)


@dlt.table(
    name="bronze_peak_unmet",
    comment="Raw peak unmet demand (MW), all columns as strings, headers sanitized.",
)
def bronze_peak_unmet():
    return read_raw_csv(ZENODO_PATH, PEAK_UNMET_FILE)


@dlt.table(
    name="bronze_weather",
    comment="Raw Open-Meteo daily weather per state, all columns as strings.",
)
def bronze_weather():
    return read_raw_csv(OPEN_METEO_PATH, WEATHER_FILE)


# ============================================================
# SILVER - typed, de-duplicated, one row per Date (energy)
# ============================================================

@dlt.table(
    name="silver_daily_energy_pipeline",
    comment="Daily energy met (MU): typed, empty regions dropped, unique per Date.",
)
@dlt.expect_or_drop("valid_date", "Date IS NOT NULL")
@dlt.expect("national_total_present", "Total IS NOT NULL")
@dlt.expect("non_negative_total", "Total IS NULL OR Total >= 0")
def silver_daily_energy_pipeline():
    return clean_energy_wide("bronze_daily_energy")


@dlt.table(
    name="silver_peak_energy_pipeline",
    comment="Peak power met (MW): typed, empty regions dropped, unique per Date.",
)
@dlt.expect_or_drop("valid_date", "Date IS NOT NULL")
@dlt.expect("national_total_present", "Total IS NOT NULL")
@dlt.expect("non_negative_total", "Total IS NULL OR Total >= 0")
def silver_peak_energy_pipeline():
    return clean_energy_wide("bronze_peak_energy")


@dlt.table(
    name="silver_peak_unmet_pipeline",
    comment="Peak unmet demand (MW): typed, empty regions dropped, unique per Date.",
)
@dlt.expect_or_drop("valid_date", "Date IS NOT NULL")
@dlt.expect("national_total_present", "Total IS NOT NULL")
# Negative unmet values exist in the source (26 entities); keep and flag them.
@dlt.expect("non_negative_total", "Total IS NULL OR Total >= 0")
def silver_peak_unmet_pipeline():
    return clean_energy_wide("bronze_peak_unmet")


@dlt.table(
    name="silver_weather_pipeline",
    comment="Open-Meteo weather: typed, unique per (Date, State).",
)
@dlt.expect_or_drop("valid_date", "Date IS NOT NULL")
@dlt.expect_or_drop("valid_state", "State IS NOT NULL AND State <> ''")
@dlt.expect("temperature_range_consistent", "temperature_2m_max >= temperature_2m_min")
@dlt.expect("non_negative_precipitation", "precipitation_sum IS NULL OR precipitation_sum >= 0")
@dlt.expect("valid_coordinates", "Latitude BETWEEN 6 AND 38 AND Longitude BETWEEN 68 AND 98")
def silver_weather_pipeline():
    df = dlt.read("bronze_weather").drop(*BRONZE_META_COLS)
    typed = df.selectExpr(
        "try_cast(`Date` as date) as `Date`",
        "trim(`State`) as `State`",
        "trim(`Location`) as `Location`",
        *[f"try_cast(`{c}` as double) as `{c}`" for c in WEATHER_NUMERIC_COLS],
    )
    return typed.groupBy("Date", "State").agg(
        F.first("Location", ignorenulls=True).alias("Location"),
        *[F.max(c).alias(c) for c in WEATHER_NUMERIC_COLS],
    )


# ============================================================
# SILVER - entity separation (states vs utilities vs national total)
# ============================================================

@dlt.table(
    name="silver_energy_long",
    comment=(
        "Daily energy, peak met and peak unmet in long format: one row per "
        "(Date, Entity). Entity_Type separates State/UT from Utility/Industrial "
        "and National Total so they are never mixed in state-wise joins."
    ),
)
@dlt.expect_or_drop("valid_entity", "Entity IS NOT NULL")
def silver_energy_long():
    daily = unpivot_entities(dlt.read("silver_daily_energy_pipeline"), "Daily_Energy_MU")
    peak = unpivot_entities(dlt.read("silver_peak_energy_pipeline"), "Peak_Met_MW")
    unmet = unpivot_entities(dlt.read("silver_peak_unmet_pipeline"), "Peak_Unmet_MW")

    keys = ["Date", "Entity"]
    joined = (
        daily.join(peak, keys, "full_outer")
        .join(unmet, keys, "full_outer")
        .filter(
            F.col("Daily_Energy_MU").isNotNull()
            | F.col("Peak_Met_MW").isNotNull()
            | F.col("Peak_Unmet_MW").isNotNull()
        )
    )

    entity_type = (
        F.when(F.col("Entity") == NATIONAL_TOTAL, ENTITY_TYPE_NATIONAL)
        .when(F.col("Entity").isin(UTILITY_ENTITIES), ENTITY_TYPE_UTILITY)
        .otherwise(ENTITY_TYPE_STATE)
    )
    return joined.withColumn("Entity_Type", entity_type)


@dlt.table(
    name="silver_missing_date_audit",
    comment="Calendar dates absent from the daily energy feed between its first and last date.",
)
def silver_missing_date_audit():
    energy = dlt.read("silver_daily_energy_pipeline").select("Date")
    bounds = energy.agg(F.min("Date").alias("first_date"), F.max("Date").alias("last_date"))
    calendar = bounds.select(F.explode(F.sequence("first_date", "last_date")).alias("Date"))
    missing = calendar.join(energy, "Date", "left_anti")
    return add_calendar_columns(missing)


@dlt.table(
    name="silver_entity_coverage",
    comment=(
        "Reporting coverage per entity: first/last reported date and gaps inside "
        "that span (Telangana, Dadra & Nagar Haveli, Daman & Diu, AMNSIL, ...)."
    ),
)
def silver_entity_coverage():
    return (
        dlt.read("silver_energy_long")
        .filter(F.col("Daily_Energy_MU").isNotNull())
        .groupBy("Entity", "Entity_Type")
        .agg(
            F.min("Date").alias("First_Date"),
            F.max("Date").alias("Last_Date"),
            F.countDistinct("Date").alias("Days_Reported"),
        )
        .withColumn("Span_Days", F.datediff("Last_Date", "First_Date") + 1)
        .withColumn("Gap_Days_In_Span", F.col("Span_Days") - F.col("Days_Reported"))
    )


# ============================================================
# GOLD - national daily totals
# ============================================================

def national_total(silver_table, value_name):
    df = dlt.read(silver_table).select("Date", F.col(NATIONAL_TOTAL).alias(value_name))
    return add_calendar_columns(df.filter(F.col(value_name).isNotNull()))


@dlt.table(name="gold_daily_energy", comment="National daily energy met (MU) for Power BI.")
def gold_daily_energy():
    return national_total("silver_daily_energy_pipeline", "Daily_Energy_Total")


@dlt.table(name="gold_peak_energy", comment="National daily peak power met (MW) for Power BI.")
def gold_peak_energy():
    return national_total("silver_peak_energy_pipeline", "Peak_Energy_Total")


@dlt.table(name="gold_peak_unmet", comment="National daily peak unmet demand (MW) for Power BI.")
def gold_peak_unmet():
    return national_total("silver_peak_unmet_pipeline", "Unmet_Energy_Total")


@dlt.table(
    name="gold_national_daily",
    comment="Combined national daily energy met (MU), peak met (MW) and peak unmet (MW), unique per Date.",
)
@dlt.expect_or_drop("valid_date", "Date IS NOT NULL")
def gold_national_daily():
    daily = dlt.read("gold_daily_energy").select("Date", "Daily_Energy_Total")
    peak = dlt.read("gold_peak_energy").select("Date", "Peak_Energy_Total")
    unmet = dlt.read("gold_peak_unmet").select("Date", "Unmet_Energy_Total")
    # Each input has one row per Date, so these full outer joins cannot multiply rows.
    return daily.join(peak, "Date", "full_outer").join(unmet, "Date", "full_outer")


# ============================================================
# GOLD - weather
# ============================================================

@dlt.table(name="gold_weather", comment="State-wise daily weather for Power BI (coordinates moved to gold_dim_state).")
def gold_weather():
    df = dlt.read("silver_weather_pipeline").select(
        "Date",
        "State",
        F.col("temperature_2m_mean").alias("Avg_Temperature"),
        F.col("temperature_2m_max").alias("Max_Temperature"),
        F.col("temperature_2m_min").alias("Min_Temperature"),
        F.col("precipitation_sum").alias("Total_Precipitation"),
        F.col("wind_speed_10m_max").alias("Avg_Wind_Speed"),
    )
    return add_calendar_columns(df)


# ============================================================
# GOLD - state grain energy (normalized) and non-state entities
# ============================================================

@dlt.table(
    name="gold_state_energy",
    comment=(
        "State/UT daily energy (MU), peak met (MW) and peak unmet (MW) at one row per "
        "(Date, State). Dadra & Nagar Haveli and Daman & Diu are merged to match "
        "weather. Excludes utilities and the national total."
    ),
)
@dlt.expect_or_drop("unique_key_not_null", "Date IS NOT NULL AND State IS NOT NULL")
def gold_state_energy():
    name_map = F.create_map(*[F.lit(x) for pair in UT_NAME_MAP.items() for x in pair])

    states = (
        dlt.read("silver_energy_long")
        .filter(F.col("Entity_Type") == ENTITY_TYPE_STATE)
        .withColumn("State", F.coalesce(name_map[F.col("Entity")], F.col("Entity")))
    )

    # Summing the two UTs is a reporting-level merge. Peak values add up to a
    # non-coincident peak, so treat merged-UT peaks as an upper bound.
    merged = states.groupBy("Date", "State").agg(
        F.sum("Daily_Energy_MU").alias("Daily_Energy_MU"),
        F.sum("Peak_Met_MW").alias("Peak_Met_MW"),
        F.sum("Peak_Unmet_MW").alias("Peak_Unmet_MW"),
    )
    return add_calendar_columns(merged, quarter=True)


@dlt.table(
    name="gold_non_state_entities",
    comment="Utility / industrial entities (AMNSIL, Damodar Valley Corporation), kept out of state analysis.",
)
def gold_non_state_entities():
    df = dlt.read("silver_energy_long").filter(F.col("Entity_Type") == ENTITY_TYPE_UTILITY)
    return add_calendar_columns(
        df.select("Date", "Entity", "Daily_Energy_MU", "Peak_Met_MW", "Peak_Unmet_MW")
    )


# ============================================================
# GOLD - state dimension
# ============================================================

@dlt.table(
    name="gold_dim_state",
    comment=(
        "State/UT dimension (36 states & UTs) with weather Location as City, "
        "coordinates (moved from weather/energy_weather), custom analytical Region "
        "mapping (STATE_REGION_MAP), and Has_Energy / Has_Weather coverage flags."
    ),
)
@dlt.expect_or_drop("valid_state", "State IS NOT NULL AND State <> ''")
@dlt.expect("valid_region", "Region IS NOT NULL")
@dlt.expect("valid_coordinates", "Latitude BETWEEN 6 AND 38 AND Longitude BETWEEN 68 AND 98")
def gold_dim_state():
    region_map = F.create_map(*[F.lit(x) for pair in STATE_REGION_MAP.items() for x in pair])

    weather_states = (
        dlt.read("silver_weather_pipeline")
        .groupBy("State")
        .agg(
            F.first("Location", ignorenulls=True).alias("City"),
            F.first("Latitude", ignorenulls=True).alias("Latitude"),
            F.first("Longitude", ignorenulls=True).alias("Longitude"),
        )
        .withColumn("Has_Weather", F.lit(True))
    )
    energy_states = (
        dlt.read("gold_state_energy")
        .select("State")
        .distinct()
        .withColumn("Has_Energy", F.lit(True))
    )

    return (
        weather_states.join(energy_states, "State", "full_outer")
        .select(
            "State",
            "City",
            region_map[F.col("State")].alias("Region"),
            "Latitude",
            "Longitude",
            F.coalesce(F.col("Has_Energy"), F.lit(False)).alias("Has_Energy"),
            F.coalesce(F.col("Has_Weather"), F.lit(False)).alias("Has_Weather"),
        )
    )


# ============================================================
# GOLD - energy + weather integration
# ============================================================

@dlt.table(
    name="gold_energy_weather",
    comment=(
        "State-wise daily energy, peak met and peak unmet joined to weather on "
        "(Date, State). Left join from energy so 2013-2021 history is kept; "
        "Has_Weather marks rows that overlap the weather feed (2022-01-01 onward)."
    ),
)
@dlt.expect("unique_date_state", "Date IS NOT NULL AND State IS NOT NULL")
def gold_energy_weather():
    energy = dlt.read("gold_state_energy").select(
        "Date", "State", "Daily_Energy_MU", "Peak_Met_MW", "Peak_Unmet_MW"
    )
    weather = dlt.read("gold_weather").drop("Year", "Month")

    joined = energy.join(weather, ["Date", "State"], "left").withColumn(
        "Has_Weather", F.col("Avg_Temperature").isNotNull()
    )
    return add_calendar_columns(joined, quarter=True)


# ============================================================
# GOLD - monthly summaries
# ============================================================

@dlt.table(
    name="gold_monthly_summary",
    comment="National monthly energy, peak met and peak unmet summary for Power BI.",
)
def gold_monthly_summary():
    df = dlt.read("gold_national_daily")

    monthly = (
        df.groupBy(F.year("Date").alias("Year"), F.month("Date").alias("Month"))
        .agg(
            F.sum("Daily_Energy_Total").alias("Total_Energy_MU"),
            F.avg("Daily_Energy_Total").alias("Avg_Daily_Energy_MU"),
            F.max("Daily_Energy_Total").alias("Max_Daily_Energy_MU"),
            F.avg("Peak_Energy_Total").alias("Avg_Peak_Met_MW"),
            F.max("Peak_Energy_Total").alias("Max_Peak_Met_MW"),
            F.avg("Unmet_Energy_Total").alias("Avg_Peak_Unmet_MW"),
            F.max("Unmet_Energy_Total").alias("Max_Peak_Unmet_MW"),
            F.count("Daily_Energy_Total").alias("Days_Reported"),
            F.sum(F.when(F.col("Unmet_Energy_Total") > 0, 1).otherwise(0)).alias("Unmet_Days"),
        )
    )
    return monthly.withColumn("Month_Start", F.make_date("Year", "Month", F.lit(1)))


@dlt.table(
    name="gold_state_monthly_summary",
    comment="Monthly state-wise energy, peak met, peak unmet and weather summary for Power BI.",
)
def gold_state_monthly_summary():
    df = dlt.read("gold_energy_weather")

    monthly = (
        df.groupBy("State", "Year", "Month")
        .agg(
            F.sum("Daily_Energy_MU").alias("Total_Energy_MU"),
            F.avg("Daily_Energy_MU").alias("Avg_Daily_Energy_MU"),
            F.avg("Peak_Met_MW").alias("Avg_Peak_Met_MW"),
            F.max("Peak_Met_MW").alias("Max_Peak_Met_MW"),
            F.avg("Peak_Unmet_MW").alias("Avg_Peak_Unmet_MW"),
            F.max("Peak_Unmet_MW").alias("Max_Peak_Unmet_MW"),
            F.sum(F.when(F.col("Peak_Unmet_MW") > 0, 1).otherwise(0)).alias("Unmet_Days"),
            F.count("Daily_Energy_MU").alias("Days_Reported"),
            F.avg("Avg_Temperature").alias("Avg_Temperature"),
            F.max("Max_Temperature").alias("Max_Temperature"),
            F.sum("Total_Precipitation").alias("Total_Precipitation"),
            F.avg("Avg_Wind_Speed").alias("Avg_Wind_Speed"),
            F.count("Avg_Temperature").alias("Weather_Days"),
        )
    )
    return monthly.withColumn("Month_Start", F.make_date("Year", "Month", F.lit(1)))
