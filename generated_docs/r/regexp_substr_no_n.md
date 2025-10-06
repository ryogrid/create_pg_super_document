# regexp_substr_no_n

## Location
[src/backend/utils/adt/regexp.c:1953-1959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1953-L1959)

## Overview
A wrapper function that delegates to  while providing an alternative SQL function signature that omits the occurrence number parameter.

## Definition

```c
Datum
regexp_substr_no_n(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a wrapper function that directly calls the main  function without any parameter modification. Like other regexp_substr wrapper functions, it exists to support PostgreSQL's function overloading system by providing different function signatures for the same underlying functionality. The function name suggests it's intended for cases where the occurrence number ('n') parameter is not specified by the SQL caller.

The function forwards all provided arguments to , which will use its default behavior for any missing parameters. This allows SQL users to call REGEXP_SUBSTR without explicitly specifying the occurrence number while still accessing the complete pattern matching functionality.

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
- Located in src/backend/utils/adt/regexp.c:1953-1959
- Part of a family of regexp_substr wrapper functions that provide different parameter combinations
- The name suggests it handles cases where the 'n' (occurrence number) parameter is omitted from SQL calls

## Simplified Source

```c
Datum
regexp_substr_no_n(PG_FUNCTION_ARGS)
{
    // Simple wrapper - delegate to main regexp_substr function
    return regexp_substr(fcinfo);
}
```