# gin_extract_tsquery_oldsig

## Location
[src/backend/utils/adt/tsginidx.c:340-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsginidx.c#L340-L349)

## Overview
A legacy stub function that maintains backward compatibility for the old signature of the GIN text search query extraction function.

## Definition
```c
Datum gin_extract_tsquery_oldsig(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a compatibility wrapper for the `gin_extract_tsquery` function. It was introduced to handle cases where code might still be using the old function signature that is "no longer considered appropriate" according to the source comments. The function simply delegates all its work to the current `gin_extract_tsquery` implementation, ensuring that existing code continues to work while using the updated internal implementation.

This is part of PostgreSQL's approach to maintaining backward compatibility during API evolution - rather than breaking existing code that might reference the old signature, a stub function with the old signature is provided that internally calls the new implementation.

## Parameters / Member Variables
The function uses `PG_FUNCTION_ARGS` macro, which provides access to:
- All the same parameters as `gin_extract_tsquery` but potentially with different type expectations from older calling code

## Dependencies
- Functions called/Symbols referenced:
  - [gin_extract_tsquery](gin_extract_tsquery.md)
- Called from (representative examples):
  - No direct references found (legacy compatibility function)

## Notes and Other Information
- This is a compatibility stub function introduced to maintain backward compatibility
- The function signature was changed at some point in PostgreSQL development, and this stub allows old code to continue working
- The function is declared in `src/backend/utils/adt/tsginidx.c` at line 340-349
- All actual functionality is delegated to the current `gin_extract_tsquery` implementation
- Part of the GIN (Generalized Inverted Index) indexing system for full-text search in PostgreSQL

## Simplified Source

```c
Datum gin_extract_tsquery_oldsig(PG_FUNCTION_ARGS) {
    // Legacy compatibility wrapper - delegates to current implementation
    return gin_extract_tsquery(fcinfo);
}
```