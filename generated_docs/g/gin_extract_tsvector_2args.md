# gin_extract_tsvector_2args

## Location
[src/backend/utils/adt/tsginidx.c:304-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsginidx.c#L304-L315)

## Overview
The gin_extract_tsvector_2args function serves as a compatibility wrapper for the older two-argument version of gin_extract_tsvector, maintaining backward compatibility with pre-9.1 contrib/tsearch2 opclass declarations.

## Definition
Datum gin_extract_tsvector_2args(PG_FUNCTION_ARGS)

## Detailed Description
This function exists solely for backward compatibility purposes. Prior to PostgreSQL 9.1, gin_extract_tsvector took only two arguments, but the current implementation requires three arguments. This wrapper function ensures that old opclass declarations from contrib/tsearch2 can still be loaded and function correctly.

The function performs a simple validation to ensure at least three arguments are provided (which should always be the case in normal operation) and then delegates to the actual gin_extract_tsvector implementation. The function includes a safety check that throws an error if fewer than three arguments are provided, though this should not happen under normal circumstances.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_NARGS (macro to get the number of function arguments)
  - [gin_extract_tsvector](gin_extract_tsvector.md) (the actual implementation function)
  - elog (for error reporting)
- Called from (representative examples):
  - No direct callers found (used by PostgreSQL function call infrastructure)

## Notes and Other Information
- This is a compatibility function intended to be removed in future versions
- Exists to support reloading of pre-9.1 contrib/tsearch2 opclass declarations
- The function comment notes that declaring it with only two arguments would cause the opr_sanity regression test to complain
- Always delegates to gin_extract_tsvector after argument validation
- Located in src/backend/utils/adt/tsginidx.c:304-315
- Should not be used in new code; use gin_extract_tsvector directly instead

## Simplified Source

```c
Datum
gin_extract_tsvector_2args(PG_FUNCTION_ARGS)
{
    // Safety check for proper argument count
    if (PG_NARGS() < 3)
        elog(ERROR, "gin_extract_tsvector requires three arguments");

    // Delegate to the actual implementation
    return gin_extract_tsvector(fcinfo);
}
```