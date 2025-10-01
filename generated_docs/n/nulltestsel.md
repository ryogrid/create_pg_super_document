# nulltestsel

## Location
[src/backend/utils/adt/selfuncs.c:1699-1783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L1699-L1783)

## Overview
Computes the selectivity of NULL test operations (IS NULL and IS NOT NULL) using statistical data about null value frequency.

## Definition

```c
Selectivity
nulltestsel(PlannerInfo *root, NullTestType nulltesttype, Node *arg,
			int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo)
```
## Detailed Description
The  function estimates the selectivity of NULL test expressions in SQL queries, specifically handling  and  operations. It provides accurate selectivity estimates based on the null fraction statistics collected during ANALYZE operations.

The function operates with three levels of information availability:

1. **Statistics Available**: When column statistics are available from ANALYZE, it uses the  field to determine the exact proportion of NULL values. For IS NULL tests, this fraction is returned directly. For IS NOT NULL tests, it returns 1.0 minus the null fraction.

2. **System Columns**: For system columns (negative varattno), which are known to never contain NULL values, it returns deterministic results: 0.0 for IS NULL and 1.0 for IS NOT NULL.

3. **No Statistics**: When no statistical information is available, it falls back to default selectivity constants (DEFAULT_UNK_SEL for IS NULL, DEFAULT_NOT_UNK_SEL for IS NOT NULL).

This function is essential for query optimization as NULL handling has significant performance implications in SQL queries, and accurate selectivity estimates help the planner choose optimal execution strategies.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : Type of null test operation (IS_NULL or IS_NOT_NULL)
- : Node representing the expression being tested for nullness
- : Relation ID to restrict analysis to (0 if no restriction)
- : Type of join operation context
- : Special join information for outer joins

## Dependencies
- Functions called/Symbols referenced:
  - [examine_variable](../e/examine_variable.md)
  - ReleaseVariableStats
  - CLAMP_PROBABILITY
  - Form_pg_statistic
  - [NullTestType](../N/NullTestType.md) (IS_NULL, IS_NOT_NULL)
- Called from (representative examples):
  - [clauselist_selectivity_ext](../c/clauselist_selectivity_ext.md)
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)
  - [GenericCosts](../G/GenericCosts.md)

## Notes and Other Information
- Uses the  statistic from ANALYZE to provide accurate null fraction estimates
- Handles system columns specially since they are guaranteed to be non-NULL
- Falls back to default constants when no statistics are available
- Ensures results are clamped to valid probability range [0.0, 1.0]
- Critical for optimizing queries with NULL checks, which are common in real-world applications
- Supports only two null test types: IS_NULL and IS_NOT_NULL

## Simplified Source

```c
Selectivity
nulltestsel(PlannerInfo *root, NullTestType nulltesttype, Node *arg,
            int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo)
{
    VariableStatData vardata;
    double selec;

    // Get variable statistics for the argument
    examine_variable(root, arg, varRelid, &vardata);

    if (HeapTupleIsValid(vardata.statsTuple))
    {
        // Use actual statistics from ANALYZE
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(vardata.statsTuple);
        double freq_null = stats->stanullfrac;

        switch (nulltesttype)
        {
            case IS_NULL:
                selec = freq_null;                // Use null fraction directly
                break;
            case IS_NOT_NULL:
                selec = 1.0 - freq_null;          // Complement of null fraction
                break;
            default:
                elog(ERROR, "unrecognized nulltesttype: %d", (int) nulltesttype);
                return (Selectivity) 0;
        }
    }
    else if (vardata.var && IsA(vardata.var, Var) &&
             ((Var *) vardata.var)->varattno < 0)
    {
        // System columns are never NULL
        selec = (nulltesttype == IS_NULL) ? 0.0 : 1.0;
    }
    else
    {
        // No statistics available, use defaults
        switch (nulltesttype)
        {
            case IS_NULL:
                selec = DEFAULT_UNK_SEL;
                break;
            case IS_NOT_NULL:
                selec = DEFAULT_NOT_UNK_SEL;
                break;
            default:
                elog(ERROR, "unrecognized nulltesttype: %d", (int) nulltesttype);
                return (Selectivity) 0;
        }
    }

    ReleaseVariableStats(vardata);
    CLAMP_PROBABILITY(selec);           // Ensure valid probability range

    return (Selectivity) selec;
}
```