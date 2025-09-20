# bytea_substr

## Location
[src/backend/utils/adt/varlena.c:3005-3018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3005-L3018)

## Overview
bytea_substr is a PostgreSQL internal function that extracts a substring from a bytea value starting at a specified position with a specified length.

## Definition

```c
Datum
bytea_substr(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides bytea substring extraction functionality with three arguments: the source bytea, starting position (1-based), and length. It serves as a wrapper around the bytea_substring function, handling the PostgreSQL function calling convention. The function follows SQL standard behavior for substring operations, including handling of zero or negative starting positions and proper length validation. It was cloned from text_substr and adapted for binary data.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: Source bytea value (as Datum)
  - Argument 1: Starting position (1-based integer)
  - Argument 2: Substring length (integer)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM (to extract bytea argument as Datum)
  - PG_GETARG_INT32 (to extract integer arguments)
  - [bytea_substring](bytea_substring.md) (performs the actual substring extraction)
  - PG_RETURN_BYTEA_P (returns the result bytea)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This is a thin wrapper around bytea_substring with fixed parameter handling
- Uses 1-based positioning following SQL standard conventions
- If starting position is zero or less, returns from the start of the string with adjusted length
- Throws an ERROR if length is negative
- The function is cloned from text_substr but adapted for binary data (bytea)
- Located in src/backend/utils/adt/varlena.c:3005-3018