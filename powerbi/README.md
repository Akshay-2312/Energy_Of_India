# GridPulse India — Power BI Semantic Model & Report (`GridPulse.pbip`)

Interactive Power BI Project (**PBIP** = **TMDL** semantic model + **PBIR** report) analyzing weather-driven electricity consumption, peak demand, and grid unmet demand (deficits) across India's states and Union Territories (`2013-01-01` to `2024-04-28`).

---

## 1. Project Layout

```text
powerbi/
├── GridPulse.pbip                  # Open this file in Power BI Desktop
├── export_gold.py                  # Reproduces all 9 Gold/Silver CSVs locally via PySpark
├── README.md                       # This documentation
├── data/                           # Exported UTF-8 CSVs (DataFolder target)
│   ├── gold_dim_state.csv
│   ├── gold_energy_weather.csv
│   ├── gold_weather.csv
│   ├── gold_national_daily.csv
│   ├── gold_monthly_summary.csv
│   ├── gold_state_monthly_summary.csv
│   ├── silver_missing_date_audit.csv
│   ├── silver_entity_coverage.csv
│   └── gold_non_state_entities.csv
├── GridPulse.SemanticModel/        # TMDL semantic model (13 tables, 10 relationships, 21 measures)
│   ├── definition.pbism
│   └── definition/
│       ├── database.tmdl
│       ├── model.tmdl
│       ├── expressions.tmdl        # DataFolder parameter
│       ├── relationships.tmdl
│       └── tables/*.tmdl
└── GridPulse.Report/               # PBIR report definition
```

---

## 2. How to Open & Refresh

### Prerequisites
- **Power BI Desktop** (March 2026 or later; PBIR and TMDL are GA/default).
- Local CSVs in `smart\powerbi\data\` (already exported; to regenerate from the 4 raw CSVs, run `export_gold.py` with the project's PySpark 4 venv).

### Opening the Project
1. Double-click `c:\mini project\smart\powerbi\GridPulse.pbip` (or launch Power BI Desktop -> **File -> Open report -> Browse** and select `GridPulse.pbip`).
2. If prompted about native queries or folder permissions, approve local file access.
3. Click **Home -> Refresh** in Power BI Desktop to load all 9 CSVs into the in-memory VertiPaq engine and compute `dim_date`, calculated band columns, and measures.

### Changing the Local Data Folder
If you move the repository to another machine or directory:
1. In Power BI Desktop, go to **Home -> Transform data -> Edit parameters**.
2. Update `DataFolder` to your local `...\powerbi\data\` path (trailing backslash optional; M queries normalize it).
3. Click **OK** -> **Apply changes**.

### CLI Inspection & Validation (`pbi-cli`)
Once `GridPulse.pbip` is open in Power BI Desktop:
```powershell
pbi connect
pbi --json model stats
pbi dax execute "EVALUATE ROW(\"Energy2023\", CALCULATE([Total Energy (MU)], dim_date[Year] = 2023))"
```

---

## 3. Repointing `DataFolder` / Model Sources to Databricks Unity Catalog

The local model reads CSVs exported by `export_gold.py` via the `DataFolder` parameter. In production on Databricks Lakeflow (`smart/smart/transformations/my_transformation.py`), the same tables live in **Unity Catalog** (`workspace.default.*`):

| Semantic Model Table | Local CSV (`DataFolder`) | Unity Catalog Table |
|---|---|---|
| `dim_state` | `gold_dim_state.csv` | `workspace.default.gold_dim_state` |
| `fact_energy_weather` | `gold_energy_weather.csv` | `workspace.default.gold_energy_weather` |
| `fact_weather` | `gold_weather.csv` | `workspace.default.gold_weather` |
| `fact_national_daily` | `gold_national_daily.csv` | `workspace.default.gold_national_daily` |
| `gold_monthly_summary` | `gold_monthly_summary.csv` | `workspace.default.gold_monthly_summary` |
| `gold_state_monthly_summary` | `gold_state_monthly_summary.csv` | `workspace.default.gold_state_monthly_summary` |
| `silver_missing_date_audit` | `silver_missing_date_audit.csv` | `workspace.default.silver_missing_date_audit` |
| `silver_entity_coverage` | `silver_entity_coverage.csv` | `workspace.default.silver_entity_coverage` |
| `gold_non_state_entities` | `gold_non_state_entities.csv` | `workspace.default.gold_non_state_entities` |

### Steps to Switch to Databricks SQL Warehouse
1. Create (or reuse) parameters `DatabricksHost` (e.g. `dbc-xxxx.cloud.databricks.com`), `DatabricksHttpPath` (e.g. `/sql/1.0/warehouses/xxxx`), and optionally `UnityCatalog` (`"workspace"`) / `UnitySchema` (`"default"`).
2. In **Power Query Editor**, replace each table's `Csv.Document(File.Contents(...))` source step with `DatabricksMultiCloud.Catalogs`:
   ```powerquery
   let
       Catalogs = DatabricksMultiCloud.Catalogs(DatabricksHost, DatabricksHttpPath, [Catalog = "workspace", Database = "default"]),
       Db = Catalogs{[Name="workspace", Kind="Database"]}[Data],
       Sch = Db{[Name="default", Kind="Schema"]}[Data],
       Tbl = Sch{[Name="gold_energy_weather", Kind="Table"]}[Data]
   in
       Tbl
   ```
3. Keep the column names identical (`Date`, `State`, etc.) so all relationships, calculated band columns, hierarchies, and `_Measures` continue working unchanged.

---

## 4. Semantic Model Dictionary

### Tables (13)

| Table | Type | Rows | Grain / Purpose | Key / Important Columns |
|---|---|---|---|---|
| `dim_date` | DAX Calculated (Marked Date Table) | 4,383 | Daily calendar `2013-01-01`..`2024-12-31` | `Date` (key), `Year`, `Quarter`, `MonthNumber`, `MonthName`, `MonthStart`, `YearMonth`, `Season`, `DayName`; Hierarchy `Calendar Hierarchy` (`Year > Quarter > Month`) |
| `dim_state` | M Import (`gold_dim_state.csv`) | 36 | All 36 weather states/UTs across 7 custom regions | `State` (key), `City`, `Region`, `Latitude`, `Longitude`, `Has_Energy`, `Has_Weather`; Hierarchy `Location Hierarchy` (`Region > State > City`) |
| `fact_energy_weather` | M Import (`gold_energy_weather.csv`) | 127,590 | Daily `(Date, State)` energy + left-joined weather | `Daily_Energy_MU`, `Peak_Met_MW`, `Peak_Unmet_MW`, `Avg_Temperature`, `Max_Temperature`, `Min_Temperature`, `Total_Precipitation`, `Avg_Wind_Speed`, `Has_Weather`; Calculated `Temperature Band`, `Rainfall Band`, `Wind Band` (+ hidden sort columns) |
| `fact_weather` | M Import (`gold_weather.csv`) | 30,564 | Daily `(Date, State)` weather for all 36 states/UTs (`2022-01-01`..`2024-04-28`) | `Avg_Temperature`, `Max_Temperature`, `Min_Temperature`, `Total_Precipitation`, `Avg_Wind_Speed` |
| `fact_national_daily` | M Import (`gold_national_daily.csv`) | 3,900 | Authoritative national daily totals (`1 row / Date`) | `Daily_Energy_Total`, `Peak_Energy_Total`, `Unmet_Energy_Total` |
| `gold_monthly_summary` | M Import (`gold_monthly_summary.csv`) | 135 | National monthly KPIs (`Month_Start` -> `dim_date[Date]`) | `Total_Energy_MU`, `Avg_Daily_Energy_MU`, `Max_Daily_Energy_MU`, `Avg_Peak_Met_MW`, `Max_Peak_Met_MW`, `Avg_Peak_Unmet_MW`, `Max_Peak_Unmet_MW`, `Days_Reported`, `Unmet_Days` |
| `gold_state_monthly_summary` | M Import (`gold_state_monthly_summary.csv`) | 4,416 | State monthly energy + weather KPIs | `Total_Energy_MU`, `Avg_Daily_Energy_MU`, `Avg_Peak_Met_MW`, `Max_Peak_Met_MW`, `Avg_Peak_Unmet_MW`, `Max_Peak_Unmet_MW`, `Unmet_Days`, `Days_Reported`, `Avg_Temperature`, `Max_Temperature`, `Total_Precipitation`, `Avg_Wind_Speed`, `Weather_Days` |
| `silver_missing_date_audit` | M Import (`silver_missing_date_audit.csv`) | 236 | Dates absent from the daily energy feed (`2013-01-01`..`2024-04-28`) | `Date`, `Year`, `Month` |
| `silver_entity_coverage` | M Import (`silver_entity_coverage.csv`) | 37 | Raw entity reporting span & gap days | `Entity`, `Entity_Type`, `First_Date`, `Last_Date`, `Days_Reported`, `Span_Days`, `Gap_Days_In_Span` |
| `gold_non_state_entities` | M Import (`gold_non_state_entities.csv`) | 7,709 | Utility/industrial entities (`AMNSIL`, `Damodar Valley Corporation`) | `Entity`, `Daily_Energy_MU`, `Peak_Met_MW`, `Peak_Unmet_MW` |
| `'Weather Driver'` | DAX Disconnected (`DATATABLE`) | 4 | Dynamic chooser for weather scatter/trend visuals | `Driver` (`Avg temperature`, `Max temperature`, `Rainfall`, `Wind`) |
| `Metric` | DAX Disconnected (`DATATABLE`) | 3 | Dynamic chooser for energy/peak visuals | `Metric` (`Energy`, `Peak met`, `Peak unmet`) |
| `_Measures` | DAX Measure Display Table | 1 row (hidden) | Houses all 21 DAX measures | See Measure Dictionary below |

### Relationships (All Many-to-One, Single Direction)
- `fact_energy_weather[Date]` -> `dim_date[Date]`
- `fact_weather[Date]` -> `dim_date[Date]`
- `fact_national_daily[Date]` -> `dim_date[Date]`
- `gold_monthly_summary[Month_Start]` -> `dim_date[Date]`
- `gold_state_monthly_summary[Month_Start]` -> `dim_date[Date]`
- `silver_missing_date_audit[Date]` -> `dim_date[Date]`
- `gold_non_state_entities[Date]` -> `dim_date[Date]`
- `fact_energy_weather[State]` -> `dim_state[State]`
- `fact_weather[State]` -> `dim_state[State]`
- `gold_state_monthly_summary[State]` -> `dim_state[State]`

### Weather Band Definitions (`fact_energy_weather`)
- **Temperature Band** (based on `Avg_Temperature` °C): `Cool (<20 C)`, `Mild (20-30 C)`, `Hot (30-38 C)`, `Extreme (>=38 C)`, `No weather data`.
- **Rainfall Band** (based on `Total_Precipitation` mm): `Dry (0 mm)`, `Light (0-10 mm)`, `Moderate (10-50 mm)`, `Heavy (>50 mm)`, `No weather data`.
- **Wind Band** (based on `Avg_Wind_Speed` km/h): `Calm (<15 km/h)`, `Breezy (15-30 km/h)`, `Strong (>=30 km/h)`, `No weather data`.

---

## 5. Measure Dictionary (`_Measures`)

> **Location Switching Rule**: Energy, peak, and unmet KPIs check `ISFILTERED` across `dim_state[Region]`, `dim_state[State]`, `dim_state[City]`, and the three weather band columns on `fact_energy_weather`. When unfiltered (All India), they read `fact_national_daily`; when filtered, they read `fact_energy_weather`.

| Measure | Display Folder | Format | Logic Summary |
|---|---|---|---|
| `Total Energy (MU)` | Energy & Demand | `#,##0.0` | `SUM` of `Daily_Energy_Total` (All India) or `Daily_Energy_MU` (state/band filtered) |
| `Avg Daily Energy (MU)` | Energy & Demand | `#,##0.0` | Average daily energy per reported day in filter context |
| `Peak Demand Met (MW)` | Energy & Demand | `#,##0` | `MAX` of `Peak_Energy_Total` or `Peak_Met_MW` |
| `Peak Unmet (MW)` | Unmet & Deficit | `#,##0` | `MAX` of `Unmet_Energy_Total` or `Peak_Unmet_MW` |
| `Unmet Days` | Unmet & Deficit | `#,##0` | Distinct reporting days with unmet demand `> 0 MW` |
| `Unmet % of Demand` | Unmet & Deficit | `0.00%` | `SUM(Unmet MW) / (SUM(Peak Met MW) + SUM(Unmet MW))` |
| `Energy YoY %` | Time Intelligence | `0.0%` | Year-over-year % change matched to active data bounds (`DATESBETWEEN` + `SAMEPERIODLASTYEAR`) |
| `Avg Temperature (C)` | Weather | `0.0` | `AVERAGE(Avg_Temperature)` (reads `fact_weather` unless a weather band is filtered) |
| `Max Temperature (C)` | Weather | `0.0` | `MAX(Max_Temperature)` |
| `Total Rainfall (mm)` | Weather | `#,##0.0` | `SUM(Total_Precipitation)` |
| `Avg Wind Speed (kmh)` | Weather | `0.0` | `AVERAGE(Avg_Wind_Speed)` |
| `Hot Days` | Weather | `#,##0` | Distinct days with `Avg_Temperature >= 30` °C |
| `Selected Driver Value` | Dynamic Choosers | `#,##0.0` | `SWITCH` on `'Weather Driver'[Driver]` |
| `Selected Metric Value` | Dynamic Choosers | `#,##0.0` | `SWITCH` on `Metric[Metric]` |
| `Selected Driver Label` | Dynamic Choosers | Text | Helper label for dynamic visual titles |
| `Selected Metric Label` | Dynamic Choosers | Text | Helper label for dynamic visual titles |
| `Header Context` | Report Metadata | Text | Active location + effective date span (e.g. `All India \| 2013-01-01 to 2024-04-28`) |
| `Last Reported Date` | Report Metadata | `yyyy-MM-dd` | Max reported date in context |
| `Data Coverage %` | Data Quality | `0.0%` | Reported days divided by inclusive calendar span days (`0.0%` for weather-only UTs) |
| `Missing Dates Count` | Data Quality | `#,##0` | `COUNTROWS(silver_missing_date_audit)` |
| `Non-State Utility Energy (MU)` | Data Quality | `#,##0.0` | `SUM(gold_non_state_entities[Daily_Energy_MU])` |

---

## 6. Data & Analytical Caveats

1. **National `Total` vs. Sum of States (~800+ discrepant days)**:
   - The Zenodo source includes an explicit national `Total` column (`fact_national_daily`) alongside individual state columns (`fact_energy_weather`).
   - On ~800+ days, `Total` differs from the sum of state rows (for example, in 2023 `fact_national_daily` totals **`1,586,382.8 MU`** while the sum of states in `fact_energy_weather` is **`1,537,179.4 MU`**, a **`49,203.4 MU`** difference). This happens because non-state utilities (`AMNSIL`, `Damodar Valley Corporation` in `gold_non_state_entities`) are excluded from state sums, plus source reporting/transmission accounting differences.
   - All-India KPIs intentionally use `fact_national_daily` when no location or weather-band filter is active, and switch to `fact_energy_weather` when slicing by Region/State/City or weather bands.

2. **Merged Union Territory (`Dadra and Nagar Haveli and Daman and Diu`)**:
   - The Zenodo energy feeds report `Dadra and Nagar Haveli` and `Daman and Diu` separately, whereas Open-Meteo weather uses the merged UT name `Dadra and Nagar Haveli and Daman and Diu`.
   - `gold_state_energy` maps both to the merged name and aggregates by `(Date, State)` using `SUM`. Summing daily energy (MU) is exact; summing `Peak_Met_MW` and `Peak_Unmet_MW` produces a non-coincident upper bound.

3. **Weather Coverage Starts `2022-01-01` & Weather-Only UTs**:
   - Energy data spans `2013-01-01` to `2024-04-28` (`3,900` reported dates, `236` missing dates in `silver_missing_date_audit`), whereas Open-Meteo weather covers `2022-01-01` to `2024-04-28` (`30,564` rows).
   - Energy rows from `2013–2021` have `Has_Weather = FALSE` and appear in `"No weather data"` bands.
   - Three UTs (`Andaman and Nicobar Islands`, `Ladakh`, `Lakshadweep`) have Open-Meteo weather (`Has_Weather = TRUE`) but no Zenodo energy data (`Has_Energy = FALSE`, empty columns dropped in Silver).

4. **Conflicting-Duplicate Date Rule in Silver**:
   - Raw Zenodo files contain repeated `Date` rows (`43` / `43` / `47` exact duplicates and `12` / `12` / `8` conflicting-value rows across daily energy, peak met, and peak unmet).
   - Silver collapses each `(Date)` (and `(Date, State)` in weather) deterministically using per-column `MAX(...)`, ignoring `NULL`s.

5. **Negative Unmet Values & Region Grouping**:
   - `26` entity records in `peak_unmet_MW.csv` contain negative values in the raw source; Silver retains and flags them (`non_negative_total` expectation).
   - `dim_state[Region]` (`North`, `South`, `East`, `West`, `Central`, `North-East`, `Islands`) is a custom analytical grouping defined in `STATE_REGION_MAP` inside `my_transformation.py`.
