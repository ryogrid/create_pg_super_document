# bytealtrim

## Location
[src/backend/utils/adt/oracle_compat.c:644-670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L644-L670)

## Overview
The  function removes bytes from the beginning (left side) of a bytea string, trimming all leading bytes that match any byte in the specified set.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that implements left trimming for bytea (binary string) data types. It removes all leading bytes from the input string that match any byte present in the trim set. The function continues removing bytes from the beginning until it encounters a byte that is not in the trim set, then returns the remaining portion of the string.

The function is implemented as a PostgreSQL V1 calling convention function, taking its arguments through the  mechanism. It delegates the actual trimming logic to the  helper function with parameters indicating that only left trimming should be performed.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The input binary string to be trimmed
  - Argument 1:  - The set of bytes to remove from the beginning of the string

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts bytea arguments from function args
  -  - Common implementation for bytea trimming operations
  -  - Returns bytea result to PostgreSQL
- Called from (representative examples):
  - No direct callers found (likely called through SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's Oracle compatibility layer, located in 
- The function calls  where the third parameter enables left trimming and the fourth parameter disables right trimming
- The trimming operation is performed byte-by-byte rather than character-by-character, making it suitable for binary data
- Returns the original string unchanged if either the input string or trim set is empty
- Uses PostgreSQL's memory management functions () for result allocation