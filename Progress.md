# GridPulse India — Project Progress & Runbook

**Project:** GridPulse India: A Medallion Lakehouse Architecture for Weather-Driven Electricity Consumption, Peak Demand, and Grid Deficit Analytics  
**Source Brief:** `Data Engineering Projects Info_-.pdf` (Domain #7: Energy – Smart Electricity Consumption Analytics) & `ProblemStatement.txt`  
**Submission Deadline:** **20 November 2026**  
**Last Updated:** 2026-11-19

---

## 1. What We Have Done So Far

### A. Project Audit & Data Profiling
- Audited `Data Engineering Projects Info_-.pdf`, `ProblemStatement.txt`, `manifest.mf`, `smart/EDA.py`, `smart/transformations/my_transformation.py`, and all four raw CSV datasets.
- Profiled the raw datasets and identified the exact data issues to solve in Bronze/Silver/Gold:
  - `all_india_weather.csv`: `30,564` rows (`2022-01-01` to `2024-04-28`, `849` days × `36` states/UTs, 0 nulls/duplicates).
  - Power CSVs (`corrected_daily_energy_met_MU.csv`, `corrected_peak_met_MW.csv`, `peak_unmet_MW.csv`): `3,955` rows × `40` columns (`2013-01-01` to `2024-04-28`), `12` column headers with spaces, `236` missing calendar dates, `55` duplicate `Date` rows per file (`43/43/47` exact duplicates + `12/12/8` conflicting-value duplicate dates), `2` completely empty columns (`Andaman and Nicobar Islands`, `Lakshadweep`), non-state entities (`AMNSIL`, `Damodar Valley Corporation`, `Total`), and UT naming mismatch (`Dadra and Nagar Haveli` + `Daman and Diu` vs merged `Dadra and Nagar Haveli and Daman and Diu` in weather; `Ladakh` only in weather).

### B. Medallion ETL Pipeline (`smart/transformations/my_transformation.py`)
- Rewrote the Databricks Lakeflow Declarative Pipeline (`dlt`) end-to-end (replacing the disconnected scaffold and manual `eda_silver_*` dependency):
  - **Bronze (4 separate Auto Loader `cloudFiles` streams):** `bronze_daily_energy`, `bronze_peak_energy`, `bronze_peak_unmet`, `bronze_weather` reading from `/Volumes/workspace/default/smartgrid_raw/{zenodo,open_meteo}/` with `pathGlobFilter`, Delta-safe header sanitization, all-string ingestion, `_ingested_at`, and `_source_file`.
  - **Silver (typed, deduplicated, quality-checked):**
    - `silver_daily_energy_pipeline`, `silver_peak_energy_pipeline`, `silver_peak_unmet_pipeline` (`3,900` unique dates each; drops empty regions; collapses duplicate `Date`s via per-column `MAX`; includes `@dlt.expect*` rules).
    - `silver_weather_pipeline` (`30,564` rows unique on `(Date, State)` with coordinate/temperature/precipitation expectations).
    - `silver_energy_long` (`142,405` rows separating `Entity_Type`: `State/UT`, `Utility/Industrial`, `National Total`).
    - `silver_missing_date_audit` (`236` missing calendar dates) and `silver_entity_coverage` (`37` entities with gap days).
  - **Gold (analytics-ready tables):**
    - `gold_dim_state` (`36` states/UTs with `City`, `Region` via `STATE_REGION_MAP`, `Latitude`, `Longitude`, `Has_Energy`, `Has_Weather`).
    - `gold_national_daily` (`3,900` unique dates) + `gold_daily_energy`, `gold_peak_energy`, `gold_peak_unmet`.
    - `gold_weather` (`30,564` rows) and `gold_non_state_entities` (`7,709` rows for `AMNSIL` and `Damodar Valley Corporation`).
    - `gold_state_energy` (`127,590` rows unpivoting daily energy, peak met, and peak unmet to `(Date, State)` and merging `Dadra and Nagar Haveli` + `Daman and Diu`).
    - `gold_energy_weather` (`127,590` rows, `27,157` with weather via left join so `2013–2021` history is preserved + `Has_Weather` flag).
    - `gold_monthly_summary` (`135` rows) and `gold_state_monthly_summary` (`4,416` rows).
- Verified the full rewritten pipeline locally with PySpark 4 (ANSI mode) against the 4 raw CSVs.

### C. Local Gold Export & Power BI Interactive Project (`powerbi/`)
- Built `powerbi/export_gold.py` to execute the pipeline locally and export **9 flat UTF-8 CSVs** into `powerbi/data/` (`0` duplicate keys, `0` orphan states).
- Built the **PBIP** project (`powerbi/GridPulse.pbip`):
  - **Semantic Model (`powerbi/GridPulse.SemanticModel/` — TMDL):** `13` tables (including DAX `dim_date` marked Date table `2013-01-01..2024-12-31`, `dim_state`, `fact_energy_weather` with sorted `Temperature Band` / `Rainfall Band` / `Wind Band`, `fact_weather`, `fact_national_daily`, monthly summaries, data-quality tables, disconnected `'Weather Driver'` and `Metric` tables, and `_Measures`), `2` drill hierarchies (`Calendar Hierarchy`, `Location Hierarchy`), `10` relationships, `DataFolder` Power Query parameter, and `21` DAX measures.
  - **Interactive Report (`powerbi/GridPulse.Report/` — PBIR):** `6` pages (`Overview`, `Weather & Demand`, `State Explorer`, hidden drill-through `State Detail`, `Unmet & Peak`, and `Data Quality`), `124` visuals, custom Hallmark Editorial Almanac theme (`gridpulse_theme.json`), compact `"GridPulse India"` header + one-line purpose subtitle + dynamic `[Header Context]` on every page, synced Date/Year/Season/Region/State/City/Weather-band slicers, `Weather Driver` and `Metric` choosers, cross-filtering, and cross-page `Reset filters` bookmarks.
- Fixed PBIR schema/resource and live Desktop issues:
  - Updated `DataFolder` M parameter (`c:\mini project\smart\powerbi\data\`) and refreshed all 9 CSV-backed tables live via `pbi-cli`.
  - Replaced tenant-blocked Azure Map with a native `Regional demand share` Pie Chart (`pieChart`), replaced broken `lineStackedColumnComboChart` and single-dot `scatterChart` visuals with populated `clusteredColumnChart` / `clusteredBarChart` visuals, enabled all 18 measure columns across all 4 `tableEx` visuals, removed the `State Tooltip` page, aligned the `Data Quality` and `State Detail` grids to `1280×720`, and applied the Hallmark Editorial Almanac × Coral design system.
  - Validated `pbi report validate` (`valid: True`, `files_checked: 139`, `0` errors) and live `pbi dax execute` on all `21` DAX measures.

---

## 2. Role of Each File in This Project

### Root Workspace (`c:\mini project\smart\`)
| File / Folder | Role |
|---|---|
| `Data Engineering Projects Info_-.pdf` | Official project instructions, evaluation rules, 10 mandatory deliverables, and submission deadline (`20 Nov 2026`). |
| `ProblemStatement.txt` | Defines the project title (*GridPulse India*), real-world context, Bronze/Silver data challenges, and 3-layer objective. |
| `manifest.mf` | Databricks Lakeflow/DLT export metadata manifest (`guid` and format version). |
| `all_india_weather.csv` | Raw Open-Meteo daily weather feed (`30,564` rows, `36` states/UTs + capital `Location` + lat/lon, `2022-01-01`..`2024-04-28`). |
| `corrected_daily_energy_met_MU.csv` | Raw Zenodo wide-format daily electricity consumption in Million Units (`3,955` rows × `40` cols, `2013-01-01`..`2024-04-28`). |
| `corrected_peak_met_MW.csv` | Raw Zenodo wide-format daily peak power met in Megawatts (`3,955` rows × `40` cols). |
| `peak_unmet_MW.csv` | Raw Zenodo wide-format daily unmet peak deficit in Megawatts (`3,955` rows × `40` cols). |
| `Progress.md` | This progress tracker, file-by-file index, Power BI runbook, and remaining checklist. |

### Databricks Pipeline & EDA (`smart/`)
| File | Role |
|---|---|
| `smart/EDA.py` | Databricks PySpark exploratory notebook (735 lines): volume inspection, null/duplicate/missing-date audit, column sanitization, and legacy `eda_silver_*` + verification cells. |
| `smart/transformations/my_transformation.py` | Main Databricks Lakeflow (`dlt`) pipeline defining all Bronze, Silver, and Gold Delta tables, expectations, entity separation, UT merging, and joins. |

### Power BI Project & Local Exporter (`powerbi/`)
| File / Folder | Role |
|---|---|
| `powerbi/GridPulse.pbip` | Power BI Project entry file linking `GridPulse.Report` and `GridPulse.SemanticModel` for Power BI Desktop. |
| `powerbi/README.md` | Detailed Power BI technical docs: table/relationship/measure dictionary, Databricks Unity Catalog swap guide, and data caveats. |
| `powerbi/export_gold.py` | Local PySpark runner that executes `my_transformation.py` logic against the 4 root CSVs and writes 9 flat CSVs to `powerbi/data/`. |
| `powerbi/.gitignore` | Ignores local Power BI Desktop runtime caches (`.pbi/cache.abf`, `localSettings.json`, etc.). |
| `powerbi/data/gold_dim_state.csv` | State/UT dimension (`36` rows: `State`, `City`, `Region`, `Latitude`, `Longitude`, `Has_Energy`, `Has_Weather`). |
| `powerbi/data/gold_energy_weather.csv` | Primary state-day fact table (`127,590` rows) combining energy, peak met, peak unmet, and left-joined weather (`Has_Weather`). |
| `powerbi/data/gold_weather.csv` | State-day weather fact table (`30,564` rows) for all 36 states/UTs (`2022-01-01`..`2024-04-28`). |
| `powerbi/data/gold_national_daily.csv` | Authoritative national daily totals (`3,900` rows: `Daily_Energy_Total`, `Peak_Energy_Total`, `Unmet_Energy_Total`). |
| `powerbi/data/gold_monthly_summary.csv` | National monthly summary (`135` rows) for long-term consumption, peak, and unmet trends. |
| `powerbi/data/gold_state_monthly_summary.csv` | State-level monthly energy + weather summary (`4,416` rows). |
| `powerbi/data/silver_missing_date_audit.csv` | Calendar dates absent from the daily energy feed (`236` rows). |
| `powerbi/data/silver_entity_coverage.csv` | Per-entity reporting span and gap-day counts (`37` rows). |
| `powerbi/data/gold_non_state_entities.csv` | Isolated industrial/utility entities (`7,709` rows: `AMNSIL`, `Damodar Valley Corporation`). |
| `powerbi/GridPulse.SemanticModel/definition.pbism` | Semantic model PBISM manifest (`version: 4.2`). |
| `powerbi/GridPulse.SemanticModel/definition/database.tmdl` | Tabular database compatibility level definition. |
| `powerbi/GridPulse.SemanticModel/definition/model.tmdl` | Model-level settings, culture (`en-US`), table order, and `TimeIntelligence` config. |
| `powerbi/GridPulse.SemanticModel/definition/expressions.tmdl` | Power Query M parameter `DataFolder` (defaults to `c:\mini project\smart\powerbi\data\`). |
| `powerbi/GridPulse.SemanticModel/definition/relationships.tmdl` | 10 many-to-one, single-direction relationships connecting facts to `dim_date` and `dim_state`. |
| `powerbi/GridPulse.SemanticModel/definition/tables/*.tmdl` | TMDL table definitions for all 13 model tables (including `dim_date.tmdl` and `_Measures.tmdl` with 21 DAX measures). *(Desktop may also generate `DateTableTemplate_*` / `LocalDateTable_*` files.)* |
| `powerbi/GridPulse.Report/definition.pbir` | Report binding referencing `../GridPulse.SemanticModel` via `byPath`. |
| `powerbi/GridPulse.Report/definition/report.json` | Report settings and `themeCollection` / `resourcePackages` declarations. |
| `powerbi/GridPulse.Report/definition/version.json` | PBIR metadata schema version (`2.0.0`). |
| `powerbi/GridPulse.Report/definition/pages/pages.json` | Report page order (`6` pages) and active landing page (`overview`). |
| `powerbi/GridPulse.Report/definition/pages/<page>/page.json` | Page layout (`1280×720`), Hallmark warm stone background (`#EFECE6`), and drill-through binding (`state_detail`). |
| `powerbi/GridPulse.Report/definition/pages/<page>/visuals/*/visual.json` | Individual visual definitions (`124` visuals total: slicers, KPIs, bar/column/pie charts, matrix, tables, buttons). |
| `powerbi/GridPulse.Report/definition/bookmarks/*.json` | 5 `Reset filters` bookmarks resetting slicers and choosers across all pages. |
| `powerbi/GridPulse.Report/StaticResources/RegisteredResources/gridpulse_theme.json` | Custom Hallmark Editorial Almanac (`GridPulse Editorial Almanac`) Power BI report theme. |
| `powerbi/GridPulse.Report/StaticResources/SharedResources/BaseThemes/CY24SU06.json` | Bundled Microsoft base theme referenced by `report.json`. |

---

## 3. How to Run This in Power BI

### Prerequisites
- **Power BI Desktop** (March 2026 or newer, with PBIR/PBIP support).
- The 9 Gold/Silver CSVs already exist under `c:\mini project\smart\powerbi\data\`.

### Steps to Open & Refresh
1. **Close any previously failed Power BI Desktop window** first so it does not keep stale state.
2. Double-click `c:\mini project\smart\powerbi\GridPulse.pbip` (or in Power BI Desktop use **File → Open report → Browse** and select `GridPulse.pbip`).
3. When the report canvas loads, click **Home → Refresh** so Power BI imports all 9 CSVs into VertiPaq and computes `dim_date`, weather bands, and all 21 measures.
4. *(If you move the folder to another PC)* Go to **Home → Transform data → Edit parameters**, update `DataFolder` to your new `...\powerbi\data\` folder path, and click **Apply changes**.
5. *(Optional: connect to Databricks later)* Follow Section 3 of `powerbi/README.md` to swap `Csv.Document(File.Contents(...))` in Power Query to `DatabricksMultiCloud.Catalogs(...)` targeting `workspace.default.*`.

### Using the Interactive Features
- **Date, Year & Season:** Use the **Date Range** slider, **Year**, and **Season** dropdowns (synced across pages). Note: energy covers `2013-01-01`–`2024-04-28`; weather covers `2022-01-01`–`2024-04-28`.
- **Region, State & City:** Filter via **Region**, searchable multi-select **State / UT**, and **City** (state capital / weather station).
- **Weather Bands:** Slice by **Temperature Band**, **Rainfall Band**, and **Wind Band**.
- **Dynamic Choosers:** Switch **Chart Metric** (`Energy`, `Peak met`, `Peak unmet`) and **Weather Driver** (`Avg temperature`, `Max temperature`, `Rainfall`, `Wind`) to dynamically re-drive trends and scatter plots.
- **Drill-Down, Drill-Through & Reset:** Use chart drill-down (`Year > Quarter > Month` and `Region > State > City`), right-click any state → **Drill through → State Detail**, hover visuals for the **State Tooltip**, and click **Clear All Filters** to reset all pages.

### Optional CLI Verification (`pbi-cli`)
Once `GridPulse.pbip` is open and refreshed in Power BI Desktop:
```powershell
pbi connect
pbi --json model stats
'EVALUATE ROW("Energy2023", CALCULATE([Total Energy (MU)], dim_date[Year] = 2023), "PeakMet", [Peak Demand Met (MW)], "PeakUnmet", [Peak Unmet (MW)])' | pbi dax execute -
```

---

## 4. Steps Left to Complete the Project

Per `Data Engineering Projects Info_-.pdf` (deadline **20 November 2026**), the remaining tasks are:

1. **Verify Refreshed Power BI Dashboard & Capture Screenshots**
   - Open `powerbi/GridPulse.pbip`, run **Home → Refresh**, verify all 7 pages render with populated KPIs/charts, test all slicers/drill-through/bookmarks, and save screenshots (also confirm Azure Map is enabled in Desktop if needed; the paired Ranked Bar chart works regardless).
2. **Deploy & Run `my_transformation.py` in Databricks**
   - Upload the 4 raw CSVs to `/Volumes/workspace/default/smartgrid_raw/zenodo/` and `.../open_meteo/`.
   - Run the Lakeflow Declarative Pipeline (`smart/transformations/my_transformation.py`) in Databricks, confirm Bronze row counts (`3,955` each for energy, `30,564` for weather) and all Gold Delta tables in `workspace.default.*`, and take pipeline/lineage screenshots.
   - *(Optional)* Clean up `smart/EDA.py` so the post-pipeline verification cells don't fail if executed top-to-bottom before the DLT pipeline runs.
3. **Add Data Extraction / API Scripts**
   - Add Python scripts (and source metadata) documenting/reproducing how the Zenodo energy CSVs and Open-Meteo weather CSV were fetched and uploaded to Unity Catalog Volumes (Deliverable #2 & #3).
4. **Create Project Architecture Diagram**
   - Create a visual architecture diagram showing Raw Sources (Zenodo + Open-Meteo) → Unity Catalog Volumes → Bronze (`cloudFiles`) → Silver (Cleaning, Deduplication, Entity Separation) → Gold → Power BI Dashboard (Deliverable #8).
5. **Set Up GitHub Repository & Root `README.md`**
   - Initialize Git (`git init`), add a root `.gitignore` and root `README.md` with project overview, architecture, setup steps, team member roles, and explicit source/API/tool attributions (Deliverable #9 & Section 6).
6. **Prepare Final Report & Presentation Deck**
   - Compile the final project report (including problem statement, data cleaning findings, Databricks pipeline screenshots, and Power BI dashboard screenshots) and team presentation slides so each team member can explain their end-to-end contribution (Deliverable #10 & Section 2).
