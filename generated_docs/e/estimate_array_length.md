# estimate_array_length

## Location
[src/backend/utils/adt/selfuncs.c:2140-2205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2140-L2205)

## Overview
Estimates the number of elements in an array expression, providing a crucial component for PostgreSQL's query planner to make cost-based decisions about array operations.

## Definition

```c
double
estimate_array_length(PlannerInfo *root, Node *arrayexpr)
```
## Detailed Description
This function analyzes an array expression and returns an estimate of how many elements it contains. The estimation process follows a hierarchical approach:

1. **Constant arrays**: For constant array expressions, it directly calculates the actual number of elements using array metadata.
2. **ArrayExpr nodes**: For single-dimensional ArrayExpr nodes (not multidimensional), it counts the elements in the expression list.
3. **Statistical estimation**: For other expressions with available statistics, it examines variable statistics to find the average element count from DECHIST statistics.
4. **Default fallback**: When no better information is available, it returns a default estimate of 10 elements.

The function is designed to work even when the planner info () is NULL, though this results in slightly less accurate estimates. The result is returned as a double to avoid integer overflow concerns and to match the data types used in cost estimation calculations.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state; can be NULL but results in less accurate estimates
- : Node representing the array expression to be analyzed

## Dependencies
- Functions called/Symbols referenced:
  - [strip_array_coercion](../s/strip_array_coercion.md)
  - DatumGetArrayTypeP
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - ARR_NDIM
  - ARR_DIMS
  - [examine_variable](examine_variable.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [clamp_row_est](../c/clamp_row_est.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - ReleaseVariableStats
- Called from (representative examples):
  - [cost_tidscan](../c/cost_tidscan.md)
  - [cost_qual_eval_walker](../c/cost_qual_eval_walker.md)
  - [array_unnest_support](../a/array_unnest_support.md)
  - [genericcostestimate](../g/genericcostestimate.md)
  - [btcostestimate](../b/btcostestimate.md)

## Notes and Other Information
- The function strips any binary-compatible relabeling of the array expression before analysis
- For statistical estimation, it uses the DECHIST (distinct element count histogram) statistics type
- The default estimate of 10 elements is chosen to match the behavior in scalararraysel
- Results are clamped using clamp_row_est to ensure reasonable bounds for planning purposes
- The function is critical for cost estimation in operations involving arrays, particularly in index scans and function calls

## Simplified Source

```c
double
estimate_array_length(PlannerInfo *root, Node *arrayexpr)
{
    // Strip any array coercion wrapping
    arrayexpr = strip_array_coercion(arrayexpr);

    // Case 1: Constant array - get actual element count
    if (arrayexpr && IsA(arrayexpr, Const))
    {
        Datum arraydatum = ((Const *) arrayexpr)->constvalue;
        bool arrayisnull = ((Const *) arrayexpr)->constisnull;

        if (arrayisnull)
            return 0;
        ArrayType *arrayval = DatumGetArrayTypeP(arraydatum);
        return ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval));
    }

    // Case 2: Single-dimensional ArrayExpr - count elements
    else if (arrayexpr && IsA(arrayexpr, ArrayExpr) &&
             !((ArrayExpr *) arrayexpr)->multidims)
    {
        return list_length(((ArrayExpr *) arrayexpr)->elements);
    }

    // Case 3: Use statistics if available
    else if (arrayexpr && root)
    {
        VariableStatData vardata;
        AttStatsSlot sslot;
        double nelem = 0;

        // Examine variable statistics
        examine_variable(root, arrayexpr, 0, &vardata);
        if (HeapTupleIsValid(vardata.statsTuple))
        {
            // Get average element count from DECHIST statistics
            if (get_attstatsslot(&sslot, vardata.statsTuple,
                                 STATISTIC_KIND_DECHIST, InvalidOid,
                                 ATTSTATSSLOT_NUMBERS))
            {
                if (sslot.nnumbers > 0)
                    nelem = clamp_row_est(sslot.numbers[sslot.nnumbers - 1]);
                free_attstatsslot(&sslot);
            }
        }
        ReleaseVariableStats(vardata);

        if (nelem > 0)
            return nelem;
    }

    // Default fallback estimate
    return 10;
}
```