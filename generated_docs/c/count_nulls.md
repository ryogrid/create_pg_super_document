# count_nulls

## Location
[src/backend/utils/adt/misc.c:76-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L76-L161)

## Overview
A static helper function that counts the number of null values in function arguments for PostgreSQL's num_nulls() and num_nonnulls() functions, supporting both variadic array and separate argument calling conventions.

## Definition


## Detailed Description
The count_nulls function is a common subroutine that analyzes function call information to determine the total number of arguments and count how many of them are NULL values. It handles two different calling patterns:

1. **Variadic array mode**: When arguments are passed as a single VARIADIC array, it examines the array's null bitmap to count null elements
2. **Separate arguments mode**: When arguments are passed individually, it iterates through each argument checking for null values

The function returns a boolean indicating success (true) or failure (false). On success, it populates the provided output parameters with the total argument count and null count. If a VARIADIC array argument itself is null, the function returns false since it cannot determine meaningful information about element nullability.

## Parameters / Member Variables
- : Function call information structure containing argument data and metadata
- : Output parameter - pointer to store the total number of arguments processed
- : Output parameter - pointer to store the count of null arguments found

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_variadic](../g/get_fn_expr_variadic.md)
  - PG_NARGS
  - PG_ARGISNULL
  - [get_base_element_type](../g/get_base_element_type.md)
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - PG_GETARG_ARRAYTYPE_P
  - ARR_NDIM
  - ARR_DIMS
  - ArrayGetNItems
  - ARR_NULLBITMAP
- Called from (representative examples):
  - [pg_num_nulls](../p/pg_num_nulls.md)
  - [pg_num_nonnulls](../p/pg_num_nonnulls.md)

## Notes and Other Information
- This is a static function internal to misc.c, not exposed in the public API
- The function uses PostgreSQL's array infrastructure to efficiently process variadic arguments
- Null bitmap processing uses bit manipulation to check individual array element nullability
- The variadic array handling includes assertion checks to ensure type safety
- Returns false (indicating failure) only when a VARIADIC array argument itself is null, making element analysis impossible