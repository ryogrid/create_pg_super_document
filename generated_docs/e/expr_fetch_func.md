# expr_fetch_func

## Location
[src/backend/statistics/extended_stats.c:2234-2249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L2234-L2249)

## Overview
A specialized data fetch function that retrieves expression evaluation results from pre-computed Datum arrays for statistics computation.

## Definition

```c
static Datum
expr_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull)
```
## Detailed Description
This function serves as a callback for the statistics computation infrastructure, providing access to pre-evaluated expression results stored in Datum arrays. Unlike typical tuple-based data access, this function operates on flattened arrays of expression values and null indicators that have been pre-computed by evaluate expressions against sample rows. The function uses the rowstride field to handle potential multi-column statistics scenarios and properly sets the null indicator for the statistics computation engine.

## Parameters / Member Variables
- : VacAttrStats pointer containing the pre-computed expression values and metadata
- : Zero-based row number to fetch data for
- : Output parameter set to indicate whether the retrieved value is null

## Dependencies
- Functions called/Symbols referenced:
  - [VacAttrStatsP](../V/VacAttrStatsP.md)
  - [AnlExprData](../A/AnlExprData.md) (indirectly through the data structure)
- Called from (representative examples):
  - [compute_expr_stats](../c/compute_expr_stats.md)

## Notes and Other Information
This function is specifically designed for expression statistics computation where data has been pre-evaluated and stored in arrays rather than constructed as tuples. The rowstride mechanism allows for flexible data organization, typically set to 1 for single-column expression statistics. The function provides an efficient interface between the pre-computed expression evaluation results and PostgreSQL's standard statistics computation routines that expect a fetch function callback interface.

## Simplified Source

```c
static Datum expr_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull) {
    // Calculate array index based on row number and stride
    int i = rownum * stats->rowstride;

    // Return the value and null status from pre-computed arrays
    *isNull = stats->exprnulls[i];
    return stats->exprvals[i];
}
```