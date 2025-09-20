# test_support_func

## Location
[src/test/regress/regress.c:1030-1102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L1030-L1102)

## Overview
A PostgreSQL test function that implements a support function interface for testing query planning support mechanisms, specifically handling selectivity estimation, cost estimation, and row count estimation.

## Definition

```c
structure */
PG_FUNCTION_INFO_V1(test_enc_setup);
```
## Detailed Description
This function serves as a comprehensive test implementation of PostgreSQL's support function interface, which is used by the query planner to obtain better estimates for selectivity, cost, and row counts of functions and operators. The function handles three main types of support requests:

1. **Selectivity Estimation**: For boolean-returning functions (assuming int4eq target), it calculates selectivity for both join and restriction cases using PostgreSQL's built-in selectivity functions.

2. **Cost Estimation**: Provides generic cost estimates for function execution, setting startup cost to 0 and per-tuple cost to 2 times the CPU operator cost.

3. **Row Count Estimation**: For set-returning functions (assuming generate_series_int4 target), it estimates the number of rows that will be returned by analyzing constant arguments and calculating the range.

The function demonstrates how support functions can provide the query planner with more accurate information than default estimates, leading to better query optimization decisions.

## Parameters / Member Variables
- : A Node pointer passed as the first argument, representing the support request from the query planner

## Dependencies
- Functions called/Symbols referenced:
  - IsA: Type checking macro for Node types
  - [join_selectivity](../j/join_selectivity.md): Calculates selectivity for join operations
  - [restriction_selectivity](../r/restriction_selectivity.md): Calculates selectivity for restriction clauses
  - linitial/lsecond: List access macros for getting first and second elements
  - [DatumGetInt32](../D/DatumGetInt32.md): Converts Datum to int32 value
  - PG_GETARG_POINTER/PG_RETURN_POINTER: PostgreSQL function interface macros
- Called from (representative examples):
  - Referenced by test_fdw_handler at src/test/regress/regress.c:1028

## Notes and Other Information
- This function is part of PostgreSQL's regression test suite located in 
- The function handles three distinct support request types: SupportRequestSelectivity, SupportRequestCost, and SupportRequestRows
- For selectivity estimation, it assumes the target function is int4eq (integer equality)
- For row estimation, it assumes the target function is generate_series_int4
- The function demonstrates best practices for implementing PostgreSQL support functions
- Support functions are crucial for query optimization in PostgreSQL, allowing custom functions to provide planner hints
- The implementation shows proper handling of different request types and safe type checking using IsA macro
- Cost estimation uses cpu_operator_cost as a baseline, which is a PostgreSQL configuration parameter