# get_variable_numdistinct

## Location
src/backend/utils/adt/selfuncs.c: 5830 - 5962

## Overview
Estimates the number of distinct values for a variable using statistical data from pg_statistic, with special handling for specific data types and fallback strategies when statistics are unavailable.

## Definition

```c
double
get_variable_numdistinct(VariableStatData *vardata, bool *isdefault)
```
## Detailed Description
This function provides a comprehensive approach to estimating the number of distinct values (cardinality) for a database column or expression. It employs multiple strategies in order of preference:

1. **pg_statistic Data**: Uses stadistinct and stanullfrac values from the statistics table when available
2. **Special Data Types**: Applies hardcoded estimates for known types (e.g., boolean columns have 2 distinct values)
3. **System Columns**: Handles special system attributes like ctid (unique) and tableoid (single value)
4. **VALUES Clauses**: Assumes uniqueness for VALUES RTE columns
5. **Uniqueness Constraints**: Overrides statistics when unique indexes or DISTINCT clauses are known
6. **Fallback Estimates**: Uses relation size or default values when no better information is available

The function handles both absolute counts (positive stadistinct) and relative estimates (negative stadistinct representing fraction of total rows).

## Parameters / Member Variables
- : VariableStatData structure containing statistical information, relation metadata, and variable details
- : Output parameter set to true when the result is based on default assumptions rather than meaningful statistical data

## Dependencies
- Functions called/Symbols referenced:
  - clamp_row_est (ensures result is within reasonable bounds)
  - DEFAULT_NUM_DISTINCT (fallback constant for unknown distributions)
  - BOOLOID (boolean data type identifier)
  - RTE_VALUES (VALUES clause range table entry type)
  - SelfItemPointerAttributeNumber (ctid system column)
  - TableOidAttributeNumber (tableoid system column)
- Called from (representative examples):
  - var_eq_const (variable equality selectivity estimation)
  - var_eq_non_const (variable equality with non-constant)
  - ineq_histogram_selectivity (inequality selectivity estimation)
  - eqjoinsel (equality join selectivity)
  - add_unique_group_var (group estimation)
  - estimate_hash_bucket_stats (hash join statistics)

## Notes and Other Information
- Always returns a positive value as callers may divide by the result or compare to exact counts
- For boolean columns, hardcoded to return 2.0 distinct values regardless of actual data distribution
- Handles unique constraints by adjusting for null fraction: stadistinct = -1.0 * (1.0 - stanullfrac)
- Uses clamp_row_est to ensure the result is within reasonable bounds and properly formatted
- The isdefault parameter helps callers understand the reliability of the estimate for decision-making
- For small tables (fewer than DEFAULT_NUM_DISTINCT rows), assumes all values are distinct
- System columns receive special treatment based on their known characteristics (ctid is unique, tableoid is constant)