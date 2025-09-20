# path_add

## Location
[src/backend/utils/adt/geo_ops.c:4348-4395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4348-L4395)

## Overview
Concatenates two open paths into a single path by joining all points from both paths sequentially.

## Definition

```c
Datum
path_add(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function performs path concatenation on two PATH objects, combining their points into a single new path. This operation is only valid for open paths - if either input path is closed, the function returns NULL. The function creates a new PATH structure containing all points from the first path followed by all points from the second path, maintaining the original coordinate order.

The function includes overflow protection to prevent integer overflow when calculating the required memory size for the combined path. Memory allocation is handled using PostgreSQL's palloc system.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  - : First PATH argument (must be open)  
  - : Second PATH argument (must be open)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P (macro for retrieving PATH arguments)
  - PG_RETURN_PATH_P (macro for returning PATH result)
  - SET_VARSIZE (macro for setting variable size)
  - [palloc](palloc.md) (PostgreSQL memory allocation)
  - ereport (PostgreSQL error reporting)
- Called from:
  - No direct references found in the codebase

## Notes and Other Information
- Only operates on open paths; returns NULL if either input path is closed
- Includes integer overflow protection when calculating memory requirements
- The resulting path inherits the 'closed' status from the first path (always false for valid operations)
- Points from the first path appear first in the result, followed by points from the second path
- Used as part of PostgreSQL's geometric data type operations for 2D paths