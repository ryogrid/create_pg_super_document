# ltrim1

## Location
[src/backend/utils/adt/oracle_compat.c:718-745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L718-L745)

## Overview
The  function removes whitespace characters (specifically spaces) from the beginning (left side) of a text string, providing a simplified version of  with a fixed trim set.

## Definition

```c
Datum
ltrim1(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that implements left whitespace trimming for text data types. It is a specialized variant of the  function with the trim set fixed to contain only the space character (). This function removes all leading space characters from the input string and returns the remaining portion.

The function is implemented as a PostgreSQL V1 calling convention function and delegates the actual trimming logic to the  helper function, passing a hardcoded space character as the trim set. This provides better performance compared to the general  function when only space trimming is needed.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The input text string to be trimmed of leading spaces

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts text argument from function args
  -  - Gets pointer to text data
  -  - Gets text length excluding header
  -  - Common implementation for text trimming operations
  -  - Returns text result to PostgreSQL
- Called from (representative examples):
  - No direct callers found (likely called through SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's Oracle compatibility layer, located in 
- The function calls  where the trim set is hardcoded to a single space character
- Provides a performance optimization over the general  function when only space trimming is required
- The comment in the source code indicates this is "ltrim with set fixed as ' '"
- Like other trim functions, it properly handles multibyte character encodings through the  implementation
- Commonly used in data cleaning operations where leading whitespace removal is needed
- Returns the original string unchanged if it contains no leading spaces