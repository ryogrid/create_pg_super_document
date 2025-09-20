# rtrim

## Location
[src/backend/utils/adt/oracle_compat.c:746-765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L746-L765)

## Overview
The  function removes characters from the end (right side) of a text string, trimming all trailing characters that match any character in the specified set.

## Definition

```c
Datum
rtrim(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that implements right trimming for text data types. It removes all trailing characters from the input string that match any character present in the trim set. The function continues removing characters from the end until it encounters a character that is not in the trim set, then returns the remaining portion of the string.

The function is implemented as a PostgreSQL V1 calling convention function and delegates the actual trimming logic to the  helper function. The  function handles both single-byte and multibyte character encodings correctly, ensuring proper character boundary detection in UTF-8 and other multibyte encodings.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The input text string to be trimmed
  - Argument 1:  - The set of characters to remove from the end of the string

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts text arguments from function args
  -  - Gets pointer to text data
  -  - Gets text length excluding header
  -  - Common implementation for text trimming operations
  -  - Returns text result to PostgreSQL
- Called from (representative examples):
  - No direct callers found (likely called through SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's Oracle compatibility layer, located in 
- The function calls  where the fifth parameter disables left trimming and the sixth parameter enables right trimming
- The  implementation properly handles multibyte character encodings by building character arrays when necessary
- For single-byte encodings, a more efficient byte-by-byte comparison is used
- Returns the original string unchanged if either the input string or trim set is empty
- Complements the  function which performs left trimming, and both are used by  for both-sides trimming
- The trimming operation respects character boundaries in multibyte encodings like UTF-8
- Commonly used in data cleaning operations to remove trailing unwanted characters