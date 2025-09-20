# bytea_substr_no_len

## Location
[src/backend/utils/adt/varlena.c:3019-3027](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3019-L3027)

## Overview
bytea_substr_no_len is a PostgreSQL internal function that extracts a substring from a bytea value starting at a specified position without requiring a length parameter.

## Definition

```c
Datum
bytea_substr_no_len(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides a variant of bytea substring extraction that only requires a starting position and automatically extracts from that position to the end of the bytea value. It serves as a wrapper around bytea_substring, specifically designed to avoid opr_sanity failures that occur when one function accepts different numbers of arguments. The function passes -1 as the length and true as the no_len parameter to bytea_substring.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: Source bytea value (as Datum)
  - Argument 1: Starting position (1-based integer)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM (to extract bytea argument as Datum)
  - PG_GETARG_INT32 (to extract starting position)
  - [bytea_substring](bytea_substring.md) (performs the actual substring extraction with no_len=true)
  - PG_RETURN_BYTEA_P (returns the result bytea)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function exists primarily to handle PostgreSQL's operator sanity checks
- Passes hardcoded -1 as length and true as no_len flag to bytea_substring
- Automatically extracts from the starting position to the end of the bytea
- Part of PostgreSQL's function overloading mechanism for substring operations
- Located in src/backend/utils/adt/varlena.c:3019-3027