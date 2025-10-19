# gin_tsquery_consistent_oldsig

## Location
[src/backend/utils/adt/tsginidx.c:350-353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsginidx.c#L350-L353)

## Overview
A legacy stub function that maintains backward compatibility for the old signature of the GIN text search query consistency checking function.

## Definition
```c
Datum gin_tsquery_consistent_oldsig(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a compatibility wrapper for the `gin_tsquery_consistent` function. Similar to `gin_extract_tsquery_oldsig`, it was introduced to handle cases where code might still be using the old function signature that is "no longer considered appropriate" according to the source comments. The function simply delegates all its work to the current `gin_tsquery_consistent` implementation, ensuring that existing code continues to work while using the updated internal implementation.

The consistency function is a critical part of the GIN indexing system - it determines whether a given set of index entries is consistent with (matches) a text search query. This stub ensures that older code calling the consistency check with the old signature continues to function properly.

## Parameters / Member Variables
The function uses `PG_FUNCTION_ARGS` macro, which provides access to:
- All the same parameters as `gin_tsquery_consistent` but potentially with different type expectations from older calling code

## Dependencies
- Functions called/Symbols referenced:
  - [gin_tsquery_consistent](gin_tsquery_consistent.md)
- Called from (representative examples):
  - No direct references found (legacy compatibility function)

## Notes and Other Information
- This is a compatibility stub function introduced to maintain backward compatibility
- The function signature was changed at some point in PostgreSQL development, and this stub allows old code to continue working
- The function is declared in `src/backend/utils/adt/tsginidx.c` at line 350-353
- All actual functionality is delegated to the current `gin_tsquery_consistent` implementation
- Part of the GIN (Generalized Inverted Index) indexing system for full-text search in PostgreSQL
- Works in conjunction with `gin_extract_tsquery_oldsig` to provide complete backward compatibility for the old GIN text search API

## Simplified Source

```c
Datum
gin_tsquery_consistent_oldsig(PG_FUNCTION_ARGS)
{
    // Simple delegation to current implementation
    return gin_tsquery_consistent(fcinfo);
}
```