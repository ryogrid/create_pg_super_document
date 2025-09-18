# MCVItem

## Location
src/include/statistics/statistics.h: 78 - 84

## Overview
MCVItem represents a single entry in a multivariate most-common values (MCV) list, storing a combination of attribute values along with their frequency and null status information.

## Definition


## Detailed Description
MCVItem is a fundamental component of PostgreSQL's multivariate MCV (most-common values) statistics. It represents a single combination of values across multiple columns, along with statistical information about how frequently this combination appears in the data. This structure is essential for the query planner to understand the distribution of value combinations across correlated columns.

Each MCVItem stores both the actual frequency of occurrence and a calculated base frequency that assumes column independence. The difference between these frequencies indicates correlation strength. The structure uses arrays of Datum values and boolean null flags to accommodate variable numbers of columns with different data types.

This information is crucial for improving selectivity estimates when multiple columns are involved in query predicates, especially when those columns are correlated.

## Parameters / Member Variables
- : The actual observed frequency (0.0-1.0) of this specific combination of values in the dataset
- : The theoretical frequency if the columns were statistically independent, used for correlation analysis
- : Array of boolean flags indicating which attributes in this combination are NULL values
- : Array of Datum values representing the actual values for each attribute in this combination

## Dependencies
- Functions called/Symbols referenced:
  - Datum (PostgreSQL's generic data type)
  - [bool](../b/bool.md) (standard boolean type)

- Called from (representative examples):
  - [statext_mcv_build](../s/statext_mcv_build.md) (constructs MCV lists containing MCVItems)
  - [statext_mcv_serialize](../s/statext_mcv_serialize.md) (serializes MCVItems for storage)
  - [statext_mcv_deserialize](../s/statext_mcv_deserialize.md) (deserializes MCVItems from storage)
  - mcv_get_match_bitmap (matches MCVItems against query predicates)
  - [pg_stats_ext_mcvlist_items](../p/pg_stats_ext_mcvlist_items.md) (exposes MCVItems through system views)
  - [MCVList](MCVList.md) (container structure that holds arrays of MCVItems)

## Notes and Other Information
- Part of PostgreSQL's extended statistics system for multivariate MCV analysis
- Used by the query planner to improve cardinality estimates for complex predicates
- The frequency values are normalized probabilities (sum to ≤ 1.0 across all items in a list)
- Base frequency comparison helps identify correlation effects between columns
- Maximum number of MCV items is limited by STATS_MCVLIST_MAX_ITEMS (MAX_STATISTICS_TARGET)
- Values array length corresponds to the number of attributes being tracked
- Null flags array has the same length as the values array
- Essential for optimizing queries involving multiple correlated columns with specific value combinations
- Stored as part of MCVList structures in the pg_statistic_ext_data system catalog