# interview_drills

15 must-know patterns for live coding; not an exam section.

One notebook per pattern, chained 01 → 15 in the `interview_drills` job. Each notebook lists the exam objectives it supports.

| # | Notebook | Pattern |
|---|---|---|
| 01 | `01_read_explicit_schema_write_delta` | Read CSV with an explicit `StructType`, write Delta |
| 02 | `02_select_filter_withcolumn_cast_alias` | Projection, filtering, derived and cast columns |
| 03 | `03_when_otherwise` | Conditional columns with `when/otherwise` and `CASE WHEN` |
| 04 | `04_groupby_agg` | `groupBy().agg()` with several aggregates, `having` equivalents |
| 05 | `05_join_types` | inner, left, right, full, cross, left_semi, left_anti |
| 06 | `06_window_latest_record_per_key` | Latest record per key with `row_number()` |
| 07 | `07_window_top_n_per_group` | Top customers per nation by total spend |
| 08 | `08_window_lag_lead_running_total` | `lag`/`lead` and running totals with frame specs |
| 09 | `09_explode_split_arrays_structs` | `split`, `explode`, arrays, structs |
| 10 | `10_null_handling_and_dedup` | `na.fill`/`na.drop`/`coalesce`, `dropDuplicates` vs `distinct` |
| 11 | `11_date_time_functions` | Date parts, arithmetic, truncation, formatting |
| 12 | `12_merge_into_upsert` | Delta `MERGE INTO` upsert (SQL and Python API) |
| 13 | `13_repartition_vs_coalesce` | Partition counts and reading `Exchange` in `explain()` |
| 14 | `14_broadcast_join_and_aqe` | `broadcast()` hint, AQE settings and how to read the plan |
| 15 | `15_structured_streaming_available_now` | `availableNow` trigger with a checkpoint into a Delta table |
