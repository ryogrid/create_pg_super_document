# ArrayCheckBoundsSafe

## Location
[src/backend/utils/adt/arrayutils.c:127-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayutils.c#L127-L152)

## Overview
Safely validates array lower-bound values against overflow conditions with optional soft error handling.

## Definition
```c
bool ArrayCheckBoundsSafe(int ndim, const int *dims, const int *lb, struct Node *escontext)
```

## Detailed Description
ArrayCheckBoundsSafe performs the core validation of array lower-bound values to prevent integer overflow during subscript calculations. It uses PostgreSQL's safe arithmetic functions to detect overflow conditions when adding dimension sizes to lower bounds, ensuring that array subscript computations will remain within integer bounds.

The function iterates through each dimension and uses pg_add_s32_overflow() to safely test whether dims[i] + lb[i] would overflow. This prevents scenarios where large lower bounds could cause integer wraparound during array element access, which could lead to memory corruption or security vulnerabilities.

The function supports both exception-throwing mode (when escontext is NULL) and soft error handling mode (when escontext is provided), making it suitable for various use cases throughout PostgreSQL's array handling infrastructure.

## Parameters / Member Variables
- `ndim`: Number of dimensions in the array
- `dims`: Array of dimension sizes for each dimension  
- `lb`: Array of lower bound values to validate
- `escontext`: Error context for soft error handling (NULL for exception throwing)

## Dependencies
- Functions called/Symbols referenced:
  - PG_USED_FOR_ASSERTS_ONLY (compiler annotation macro)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (safe arithmetic function)
  - ereturn (error handling macro)
- Called from (representative examples):
  - [ArrayCheckBounds](ArrayCheckBounds.md)
  - AARR_LBOUND (array header macro)

## Notes and Other Information
- Returns false on overflow error when using ErrorSaveContext, true on success
- Uses pg_add_s32_overflow for safe overflow detection
- The sum variable is marked PG_USED_FOR_ASSERTS_ONLY to prevent compiler warnings
- Core safety mechanism for preventing array bounds overflow vulnerabilities  
- Provides the underlying validation logic for both soft and hard error handling scenarios
- Essential for maintaining memory safety in PostgreSQL's array operations