# hypothetical_rank_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:1244-1257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L1244-L1257)

## Overview
Implements the final phase of the PostgreSQL hypothetical-set aggregate function `rank()`, which computes the rank of a hypothetical row within an ordered dataset.

## Definition
```c
Datum hypothetical_rank_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the finalization function for the `rank()` hypothetical-set aggregate. It acts as a thin wrapper around `hypothetical_rank_common`, providing the appropriate flag value (-1) to ensure that the hypothetical row is ranked ahead of its peers when there are ties.

The `rank()` function follows SQL standard semantics where tied values receive the same rank, and subsequent ranks are adjusted by the number of tied rows. For example, if three rows tie for rank 2, the next row gets rank 5 (not rank 3).

## Parameters / Member Variables
- `fcinfo`: Standard PostgreSQL function call information structure containing both direct arguments (hypothetical row values) and aggregate state

## Dependencies
- Functions called/Symbols referenced:
  - [hypothetical_rank_common](hypothetical_rank_common.md): Core implementation for hypothetical ranking calculations
  - `PG_RETURN_INT64`: PostgreSQL macro to return int64 result
- Called from (representative examples):
  - PostgreSQL aggregate execution framework (no direct callers found in indexed code)

## Notes and Other Information
- This is part of PostgreSQL's hypothetical-set aggregate implementation for window functions
- The function is registered as an aggregate final function in the system catalogs
- Uses flag value -1 to sort the hypothetical row ahead of its peers, implementing standard rank() tie-breaking behavior
- Returns a 1-based rank value consistent with SQL standards
- The rowcount output from the common function is not used in this wrapper but is available for other ranking functions that need it
- Complements other ranking functions like `dense_rank()`, `percent_rank()`, and `cume_dist()` that use the same underlying infrastructure