# get_variable_numdistinct

## Location
[src/backend/utils/adt/selfuncs.c:5830-5962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L5830-L5962)

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
  - [clamp_row_est](../c/clamp_row_est.md) (ensures result is within reasonable bounds)
  - DEFAULT_NUM_DISTINCT (fallback constant for unknown distributions)
  - BOOLOID (boolean data type identifier)
  - RTE_VALUES (VALUES clause range table entry type)
  - SelfItemPointerAttributeNumber (ctid system column)
  - TableOidAttributeNumber (tableoid system column)
- Called from (representative examples):
  - [var_eq_const](../v/var_eq_const.md) (variable equality selectivity estimation)
  - [var_eq_non_const](../v/var_eq_non_const.md) (variable equality with non-constant)
  - [ineq_histogram_selectivity](../i/ineq_histogram_selectivity.md) (inequality selectivity estimation)
  - [eqjoinsel](../e/eqjoinsel.md) (equality join selectivity)
  - [add_unique_group_var](../a/add_unique_group_var.md) (group estimation)
  - [estimate_hash_bucket_stats](../e/estimate_hash_bucket_stats.md) (hash join statistics)

## Notes and Other Information
- Always returns a positive value as callers may divide by the result or compare to exact counts
- For boolean columns, hardcoded to return 2.0 distinct values regardless of actual data distribution
- Handles unique constraints by adjusting for null fraction: stadistinct = -1.0 * (1.0 - stanullfrac)
- Uses clamp_row_est to ensure the result is within reasonable bounds and properly formatted
- The isdefault parameter helps callers understand the reliability of the estimate for decision-making
- For small tables (fewer than DEFAULT_NUM_DISTINCT rows), assumes all values are distinct
- System columns receive special treatment based on their known characteristics (ctid is unique, tableoid is constant)

## Simplified Source

```c
double
get_variable_numdistinct(VariableStatData *vardata, bool *isdefault)
{
    double stadistinct;
    double stanullfrac = 0.0;
    double ntuples;

    *isdefault = false;

    // Try to get statistics from pg_statistic
    if (HeapTupleIsValid(vardata->statsTuple)) {
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(vardata->statsTuple);
        stadistinct = stats->stadistinct;
        stanullfrac = stats->stanullfrac;
    }
    // Special case: boolean columns have 2 distinct values
    else if (vardata->vartype == BOOLOID) {
        stadistinct = 2.0;
    }
    // VALUES clauses: assume unique
    else if (vardata->rel && vardata->rel->rtekind == RTE_VALUES) {
        stadistinct = -1.0;  // unique
    }
    // System columns have known characteristics
    else if (vardata->var && IsA(vardata->var, Var)) {
        switch (((Var *) vardata->var)->varattno) {
            case SelfItemPointerAttributeNumber:
                stadistinct = -1.0;  // ctid is unique
                break;
            case TableOidAttributeNumber:
                stadistinct = 1.0;   // tableoid is constant
                break;
            default:
                stadistinct = 0.0;   // unknown
                break;
        }
    }
    else {
        stadistinct = 0.0;  // unknown
    }

    // Override with uniqueness constraint if known
    if (vardata->isunique)
        stadistinct = -1.0 * (1.0 - stanullfrac);

    // Return absolute estimate if available
    if (stadistinct > 0.0)
        return clamp_row_est(stadistinct);

    // Get relation size for relative estimates
    if (vardata->rel == NULL || vardata->rel->tuples <= 0.0) {
        *isdefault = true;
        return DEFAULT_NUM_DISTINCT;
    }

    ntuples = vardata->rel->tuples;

    // Apply relative estimate (negative stadistinct)
    if (stadistinct < 0.0)
        return clamp_row_est(-stadistinct * ntuples);

    // Fallback: assume distinct = tuples for small tables, else default
    if (ntuples < DEFAULT_NUM_DISTINCT)
        return clamp_row_est(ntuples);

    *isdefault = true;
    return DEFAULT_NUM_DISTINCT;
}
```