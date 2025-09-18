# multirange_unnest

## Location
src/backend/utils/adt/multirangetypes.c: 2713 - 2719

## Overview
Converts a multirange into a set of individual ranges, returning each range as a separate row in a set-returning function.

## Definition


## Detailed Description
The `multirange_unnest` function is a set-returning function (SRF) that takes a multirange as input and returns each individual range within that multirange as separate result rows. This function implements the PostgreSQL SQL function `unnest()` for multirange types.

The function uses PostgreSQL's set-returning function framework, maintaining state across multiple calls using a function context structure. On the first call, it initializes the context with the input multirange and sets up iteration state. On subsequent calls, it returns the next range from the multirange until all ranges have been returned.

The implementation uses a local structure `multirange_unnest_fctx` to maintain:
- The input multirange
- Type cache information for efficient access
- Current iteration index

## Parameters / Member Variables
- **Input (via PG_FUNCTION_ARGS)**: A multirange value to be unnested
- **Return**: Individual ranges from the multirange, returned one per function call

### Internal Context Structure (multirange_unnest_fctx):
- `mr`: Pointer to the MultirangeType being processed
- `typcache`: TypeCacheEntry for efficient type operations
- `index`: Current position in the iteration through ranges

## Dependencies
- Functions called/Symbols referenced:
  - `SRF_IS_FIRSTCALL()`: PostgreSQL SRF macro for first call detection
  - `SRF_FIRSTCALL_INIT()`: Initialize SRF context
  - `PG_GETARG_MULTIRANGE_P()`: Extract multirange argument
  - [palloc](../p/palloc.md)(): PostgreSQL memory allocation
  - [lookup_type_cache](../l/lookup_type_cache.md)(): Get type cache information
  - `MultirangeTypeGetOid()`: Get OID of multirange type
  - `SRF_PERCALL_SETUP()`: Setup for each SRF call
  - [multirange_get_range](multirange_get_range.md)(): Extract specific range from multirange
  - `RangeTypePGetDatum()`: Convert range to Datum
  - `SRF_RETURN_NEXT()`: Return next result in SRF
  - `SRF_RETURN_DONE()`: Signal completion of SRF
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)(): Memory context management
- Called from: 
  - SQL `unnest()` function calls on multirange types (via function catalog)

## Notes and Other Information
- This is a set-returning function that can return multiple rows from a single function call
- The function properly manages PostgreSQL memory contexts to ensure memory is cleaned up appropriately
- Uses PostgreSQL's standard SRF (Set Returning Function) framework for iteration
- Each call returns one range until all ranges in the multirange have been exhausted
- The function handles detoasting of the input multirange value when necessary
- Located in `src/backend/utils/adt/multirangetypes.c:2713-2781`