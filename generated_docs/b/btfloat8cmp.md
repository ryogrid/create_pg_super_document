# btfloat8cmp

## Location
[src/backend/utils/adt/float.c:967-975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L967-L975)

## Overview
PostgreSQL function that provides a three-way comparison between two double-precision floating-point numbers for B-tree indexing operations.

## Definition


## Detailed Description
This function is a PostgreSQL SQL-callable wrapper around the internal float8_cmp_internal function. It extracts two double-precision floating-point arguments from the PostgreSQL function call interface and performs a three-way comparison (returning -1, 0, or 1 for less than, equal to, or greater than comparisons respectively). The function is specifically designed for use by B-tree indexes to support ordering and sorting operations on double-precision columns.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: First double-precision floating-point number (float8)
  - Argument 1: Second double-precision floating-point number (float8)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro for extracting float8 arguments)
  - [float8_cmp_internal](../f/float8_cmp_internal.md) (performs the actual comparison)
  - PG_RETURN_INT32 (macro for returning int32 result)

- Called from (representative examples):
  - No direct references found in the codebase (likely referenced through function pointer tables or system catalogs)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:967-975
- This function serves as the comparison operator for B-tree indexes on float8 (double precision) columns
- The actual comparison logic is delegated to float8_cmp_internal, which handles special cases like NaN values
- Returns an int32 value following PostgreSQL's comparison function convention (-1, 0, 1)