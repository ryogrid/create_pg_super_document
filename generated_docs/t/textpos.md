# textpos

## Location
[src/backend/utils/adt/varlena.c:1153-1175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1153-L1175)

## Overview
The  function implements the SQL POSITION() function to return the position of a specified substring within a text string.

## Definition

```c
Datum
textpos(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that implements the SQL standard POSITION() function. It takes two text arguments - a source string and a search string - and returns the 1-based position of the first occurrence of the search string within the source string. If the search string is not found, it returns 0. The function serves as a wrapper around the internal  function, providing the standard SQL interface for substring position operations.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention containing:
  - Argument 0:  - The source text string to search within
  - Argument 1:  - The substring to search for

## Dependencies
- Functions called/Symbols referenced:
  -  - Core implementation for finding substring position
  -  - Retrieves the collation for text comparison
  -  - Extracts text arguments from function call
  -  - Returns integer result

## Notes and Other Information
- Implements the SQL POSITION() function as specified in "A Guide To The SQL Standard" by Date & Darwen (1997)
- Returns 1-based position (SQL standard) rather than 0-based (C standard)
- Respects database collation settings for text comparison
- Part of PostgreSQL's variable-length data type operations in varlena.c