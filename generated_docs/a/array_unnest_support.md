# array_unnest_support

## Location
[src/backend/utils/adt/arrayfuncs.c:6333-6368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L6333-L6368)

## Overview
A planner support function that provides row count estimates for the array_unnest() function to help the PostgreSQL query planner make better optimization decisions.

## Definition

```c
Datum
array_unnest_support(PG_FUNCTION_ARGS)
```
## Detailed Description
The array_unnest_support function is a specialized planner support function designed to assist PostgreSQL's query optimizer when dealing with array_unnest() operations. It analyzes the input array argument and attempts to estimate how many rows the unnest operation will produce. This information helps the planner choose optimal execution strategies, particularly for joins and other operations involving the unnested results.

The function handles SupportRequestRows requests by examining the array argument and calling estimate_array_length() to provide a row count estimate. This same function also serves as the support function for information_schema._pg_expandarray(), which is essentially a wrapper around array_unnest().

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : A Node pointer representing the planner support request

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - [is_funcclause](../i/is_funcclause.md)
  - [estimate_expression_value](../e/estimate_expression_value.md)  
  - [estimate_array_length](../e/estimate_array_length.md)
  - linitial (macro for getting first list element)
  - PG_GETARG_POINTER
  - PG_RETURN_POINTER
- Called from (representative examples):
  - PostgreSQL planner during query optimization (via function catalog entry)

## Notes and Other Information
- This is a planner support function, not directly callable by users
- Registered in the system catalogs as a support function for array_unnest()
- Also used by information_schema._pg_expandarray() for consistency
- Returns NULL if the request type is not SupportRequestRows or if estimation fails
- Uses paranoid checking with is_funcclause() to ensure the node structure is valid
- The row estimate helps the planner make better decisions about join algorithms and memory usage