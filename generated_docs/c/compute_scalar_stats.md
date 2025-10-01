# compute_scalar_stats

## Location
[src/backend/commands/analyze.c:2356-2884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L2356-L2884)

## Overview
The comprehensive statistical analysis function for scalar data types that computes detailed column statistics including histograms, most common values, distinct value estimates, and correlation coefficients when both equality and less-than operators are available.

## Definition
```c
static void compute_scalar_stats(VacAttrStatsP stats,
                                 AnalyzeAttrFetchFunc fetchfunc,
                                 int samplerows,
                                 double totalrows)
```

## Detailed Description
The `compute_scalar_stats` function performs the most sophisticated statistical analysis available in PostgreSQL's ANALYZE operation. It is used when both equality and less-than operators are available for a data type, enabling full statistical analysis including histogram construction. The function first scans the sample data to collect sortable values, filtering out overly wide values that exceed WIDTH_THRESHOLD. It then sorts the collected values using PostgreSQL's sort support infrastructure and analyzes them to identify distinct values, compute frequencies, and generate most common values (MCV) lists. The function implements the Haas and Stokes estimator for distinct value estimation and constructs distribution histograms using evenly-spaced bucket boundaries. It also computes correlation coefficients between physical storage order and logical sort order, which helps the query planner estimate the cost of ordered operations.

## Parameters / Member Variables
- `stats`: Pointer to VacAttrStatsP structure to store computed statistics
- `fetchfunc`: Function pointer to retrieve datum values from the sample data
- `samplerows`: Number of rows in the sample to analyze
- `totalrows`: Total number of rows in the table for scaling estimates

## Dependencies
- Functions called/Symbols referenced:
  - [PrepareSortSupportFromOrderingOp](../P/PrepareSortSupportFromOrderingOp.md)
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - VARSIZE_ANY, DatumGetPointer, DatumGetCString
  - [toast_raw_datum_size](../t/toast_raw_datum_size.md), PG_DETOAST_DATUM
  - qsort_interruptible, compare_scalars
  - [analyze_mcv_list](../a/analyze_mcv_list.md)
  - [datumCopy](../d/datumCopy.md), MemoryContextSwitchTo
  - ScalarItem, ScalarMCVItem structures
- Called from (representative examples):
  - [std_typanalyze](../s/std_typanalyze.md)

## Notes and Other Information
- This is the most comprehensive analysis function, used only when both equality and less-than operators are available
- Implements sophisticated algorithms for MCV selection based on statistical significance
- Uses the Haas and Stokes Duj1 estimator: n*d / (n - f1 + f1*n/N) for distinct value estimation  
- Constructs histogram with evenly-spaced boundaries to represent data distribution
- Computes correlation coefficient between physical and logical order for query optimization
- Handles variable-length data types with special consideration for toasted values
- Filters out excessively wide values (> WIDTH_THRESHOLD) to prevent memory issues
- Uses sort support infrastructure for efficient comparison operations
- Generates up to three types of statistics: MCV lists, histograms, and correlation coefficients
- Implements memory context switching to ensure statistics are stored in the appropriate long-lived context
- Uses sophisticated duplicate detection during sorting to avoid redundant comparisons
- The correlation statistic helps the planner estimate costs for ORDER BY and similar operations

## Simplified Source

```c
static void compute_scalar_stats(VacAttrStatsP stats,
                                AnalyzeAttrFetchFunc fetchfunc,
                                int samplerows,
                                double totalrows) {
    int null_cnt = 0, nonnull_cnt = 0, toowide_cnt = 0;
    double total_width = 0;
    ScalarItem *values;
    int values_cnt = 0;
    ScalarMCVItem *track;
    int track_cnt = 0;
    int num_mcv = stats->attstattarget;
    SortSupportData ssup;

    // Allocate working arrays
    values = (ScalarItem *) palloc(samplerows * sizeof(ScalarItem));
    track = (ScalarMCVItem *) palloc(num_mcv * sizeof(ScalarMCVItem));

    // Setup sorting infrastructure
    setup_sort_support(&ssup, stats);

    // Phase 1: Scan sample data and collect sortable values
    for (int i = 0; i < samplerows; i++) {
        Datum value;
        bool isnull;

        value = fetchfunc(stats, i, &isnull);

        if (isnull) {
            null_cnt++;
            continue;
        }
        nonnull_cnt++;

        // Calculate width for variable-length types
        if (is_variable_width_type(stats)) {
            total_width += calculate_datum_width(value, stats);

            // Skip excessively wide values
            if (datum_too_wide(value)) {
                toowide_cnt++;
                continue;
            }
        }

        // Add to sortable values list
        values[values_cnt].value = value;
        values[values_cnt].tupno = values_cnt;
        values_cnt++;
    }

    // Phase 2: Sort and analyze collected values
    if (values_cnt > 0) {
        // Sort the collected values
        qsort_interruptible(values, values_cnt, sizeof(ScalarItem),
                          compare_scalars, &ssup);

        // Count distinct values and find most common values
        int ndistinct = analyze_sorted_values(values, values_cnt, track, &track_cnt);

        // Compute basic statistics
        stats->stanullfrac = (double) null_cnt / (double) samplerows;
        stats->stawidth = compute_average_width(total_width, nonnull_cnt, stats);
        stats->stadistinct = estimate_distinct_values(ndistinct, nonnull_cnt,
                                                     toowide_cnt, samplerows, totalrows);

        // Generate MCV list if significant values found
        if (track_cnt > 0) {
            generate_mcv_statistics(stats, values, track, track_cnt, samplerows);
        }

        // Generate histogram if enough distinct values
        if (ndistinct - track_cnt >= 2) {
            generate_histogram_statistics(stats, values, values_cnt, track, track_cnt);
        }

        // Generate correlation statistics
        if (values_cnt > 1) {
            generate_correlation_statistics(stats, values, values_cnt);
        }

        stats->stats_valid = true;
    }
    else if (nonnull_cnt > 0) {
        // Handle case with only too-wide values
        handle_too_wide_only_case(stats, null_cnt, total_width, nonnull_cnt, samplerows);
    }
    else if (null_cnt > 0) {
        // Handle all-null column
        handle_all_null_case(stats);
    }
}
```