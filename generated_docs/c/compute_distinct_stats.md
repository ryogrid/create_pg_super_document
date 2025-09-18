# compute_distinct_stats

## Location
[src/backend/commands/analyze.c:2013-2031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L2013-L2031)

## Overview
A static function that computes column statistics for PostgreSQL's ANALYZE command when only an equality operator is available for the data type, focusing on null fraction, average width, most common values, and estimated number of distinct values.

## Definition


## Detailed Description
compute_distinct_stats is a core statistical analysis function used by PostgreSQL's ANALYZE command for data types that only have an equality operator available. It implements a brute-force approach to determine the most common values by maintaining a list of previously seen values ordered by frequency. The function computes several key statistics including the null fraction, average column width, most common values (MCV), and an estimate of the total number of distinct values using the Haas-Stokes estimator.

The algorithm works by scanning through sample rows and maintaining a tracking list of values sorted by frequency. When a new value is encountered, it's inserted after the last multiply-seen value, causing the oldest singly-seen value to be dropped if the list is full. The function uses the Duj1 estimator formula: n*d / (n - f1 + f1*n/N) where f1 is the number of values that occurred exactly once, d is distinct values in sample, n is sample size, and N is total population.

For columns with small, fixed sets of possible values (like boolean or enum types), the function can generate complete MCV lists. Otherwise, it selectively stores only values that are significantly more common than those not in the list.

## Parameters / Member Variables
- : Pointer to VacAttrStats structure containing column metadata and where computed statistics will be stored
- : Function pointer to retrieve individual sample values from the column
- : Number of rows in the analyzed sample
- : Total number of rows in the table being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - VacAttrStatsP (parameter type)
  - AnalyzeAttrFetchFunc (function pointer type)
  - [TrackItem](../T/TrackItem.md) (internal struct for value tracking)
  - Various PostgreSQL memory management functions (palloc, datumCopy)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (for equality comparisons)
  - [analyze_mcv_list](../a/analyze_mcv_list.md) (for MCV list optimization)
- Called from (representative examples):
  - [std_typanalyze](../s/std_typanalyze.md) (main analysis function dispatcher)

## Notes and Other Information
- This function is used specifically when the data type only supports equality operations and not ordering operations
- Implements the Haas-Stokes Duj1 estimator for distinct value estimation, chosen for numerical stability when sample size is much smaller than population
- Uses a brute-force approach for MCV detection that scales with the length of the tracking list
- Handles variable-width types by computing actual widths and managing TOAST decompression
- Excludes excessively wide values from analysis to prevent memory issues
- Results are stored in the pg_statistic system catalog for use by the query planner
- The tracking list size is typically 2*n for an n-element MCV list, with a minimum of 10 entries