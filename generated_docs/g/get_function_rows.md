# get_function_rows

## Location
src/backend/optimizer/util/plancat.c: 2150 - 2207

## Overview
Estimates the number of rows returned by a set-returning function for query planning purposes.

## Definition
```c
double get_function_rows(PlannerInfo *root, Oid funcid, Node *node)
```

## Detailed Description
The get_function_rows function is a specialized component of PostgreSQL's query optimizer that estimates the cardinality (number of rows) that will be returned by a set-returning function (SRF). This information is crucial for the query planner to make informed decisions about join order, access methods, and other optimization strategies.

The function operates using a two-tier estimation approach:

1. **Support Function Method**: If the function has a registered support function (prosupport), it creates a SupportRequestRows structure and calls the support function to obtain a custom row count estimate. This allows function authors to provide specialized cardinality estimation logic based on their knowledge of the function's behavior and the specific arguments passed to it.

2. **Default Method**: If no support function exists or it fails, the function falls back to using the prorows value stored in the pg_proc system catalog. This represents a static estimate that was provided when the function was created.

The function includes an assertion to verify that the target function is indeed a set-returning function (proretset flag is true), as calling this function on a scalar function would be a programming error.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and statistics (may be NULL in some contexts)
- `funcid`: OID of the set-returning function for which to estimate row count
- `node`: Parse tree node representing the function call (typically FuncExpr or OpExpr), may be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - OidFunctionCall1
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_proc
  - SupportRequestRows
- Called from (representative examples):
  - [expression_returns_set_rows](../e/expression_returns_set_rows.md)

## Notes and Other Information
The function returns an unfiltered estimate and does not apply any clamping to ensure reasonable bounds. Callers are typically expected to apply clamp_row_est() to the result to prevent extremely large or small estimates from causing poor planning decisions. The function properly manages system catalog cache resources by releasing cached tuples after use. The row count estimation is essential for determining the cost and selectivity of operations involving set-returning functions in complex queries.