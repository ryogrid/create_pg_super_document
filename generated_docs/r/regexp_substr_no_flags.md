# regexp_substr_no_flags

## Location
[src/backend/utils/adt/regexp.c:1960-1966](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1960-L1966)

## Overview
A wrapper function that delegates to  while providing an alternative SQL function signature that omits the flags parameter.

## Definition

```c
Datum
regexp_substr_no_flags(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a wrapper function that directly calls the main  function without any parameter modification. It serves as part of PostgreSQL's function overloading mechanism, allowing SQL users to call REGEXP_SUBSTR without specifying regex flags. The function name indicates it handles cases where the flags parameter (which controls regex behavior like case-insensitive matching, multi-line mode, etc.) is omitted from SQL calls.

When flags are not provided,  uses default regex behavior without special flags. This wrapper ensures that SQL queries can use a simpler function signature while still accessing the complete pattern matching functionality of the underlying implementation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Function call information containing all passed arguments, forwarded directly to
## Dependencies
- Functions called/Symbols referenced:
  - 
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- This function exists primarily for PostgreSQL's internal function overloading system
- The comment indicates it's separated to prevent opr_sanity regression test complaints
- It's a simple pass-through function with no additional logic or parameter processing
- Located in src/backend/utils/adt/regexp.c:1960-1966
- Part of a family of regexp_substr wrapper functions that provide different parameter combinations
- The name suggests it handles cases where the flags parameter is omitted from SQL calls
- When no flags are specified, regex matching uses default behavior (case-sensitive, single-line mode, etc.)

## Simplified Source

```c
Datum
regexp_substr_no_flags(PG_FUNCTION_ARGS)
{
    // Simple wrapper - delegate to main regexp_substr function
    return regexp_substr(fcinfo);
}
```