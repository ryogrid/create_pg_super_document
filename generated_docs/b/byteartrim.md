# byteartrim

## Location
[src/backend/utils/adt/oracle_compat.c:671-697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L671-L697)

## Overview
The  function removes bytes from the end (right side) of a bytea string, trimming all trailing bytes that match any byte in the specified set.

## Definition

```c
Datum
byteartrim(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that implements right trimming for bytea (binary string) data types. It removes all trailing bytes from the input string that match any byte present in the trim set. The function continues removing bytes from the end until it encounters a byte that is not in the trim set, then returns the remaining portion of the string.

The function is implemented as a PostgreSQL V1 calling convention function, taking its arguments through the  mechanism. It delegates the actual trimming logic to the  helper function with parameters indicating that only right trimming should be performed.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The input binary string to be trimmed
  - Argument 1:  - The set of bytes to remove from the end of the string

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts bytea arguments from function args
  -  - Common implementation for bytea trimming operations
  -  - Returns bytea result to PostgreSQL
- Called from (representative examples):
  - No direct callers found (likely called through SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's Oracle compatibility layer, located in 
- The function calls  where the third parameter disables left trimming and the fourth parameter enables right trimming
- The trimming operation is performed byte-by-byte rather than character-by-character, making it suitable for binary data
- Returns the original string unchanged if either the input string or trim set is empty
- Complements the  function which performs left trimming
- Uses PostgreSQL's memory management functions () for result allocation