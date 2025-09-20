# lpad

## Location
[src/backend/utils/adt/oracle_compat.c:147-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L147-L244)

## Overview
The  function left-pads a string to a specified length with a padding string, or truncates the string if it's longer than the specified length.

## Definition

```c
Datum
lpad(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function takes three parameters: a source string, a target length, and a padding string. It returns the source string left-padded to the specified length using the padding string. If the source string is longer than the target length, it truncates the string on the right to the target length. The padding string is repeated as necessary to fill the required space. The function handles multibyte characters correctly and includes overflow protection for very large requested lengths.

## Parameters / Member Variables
- : PostgreSQL function argument structure containing:
  - Input parameter 0:  - The source string to be padded or truncated
  - Input parameter 1:  - The target length for the result string
  - Input parameter 2:  - The padding string to use for left-padding

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract text arguments from function arguments
  -  - Extract integer argument
  -  - Get size of variable-length data excluding header
  -  - Get pointer to variable-length data
  -  - Calculate multibyte string length
  -  - Get maximum bytes per character for database encoding
  -  - Safe 32-bit multiplication with overflow check
  -  - Safe 32-bit addition with overflow check
  -  - Validate memory allocation size
  -  - Report errors
  -  - Allocate memory
  -  - Get pointer to variable-length data
  -  - Get length of multibyte character
  -  - Copy memory
  -  - Set size of variable-length data
  -  - Return text result to PostgreSQL

- Called from (representative examples):
  - SQL queries using the  function
  - PostgreSQL query executor

## Notes and Other Information
- Located in  at lines 147-244
- Part of PostgreSQL's Oracle compatibility functions
- Handles multibyte characters correctly by using  and related functions
- Includes overflow protection to prevent memory allocation issues with extremely large lengths
- Negative target lengths are silently treated as zero
- If the padding string is empty, no padding is performed (result length equals source string length or target length, whichever is smaller)
- The padding string is repeated cyclically if needed to fill the required space
- Memory is properly allocated and the result is returned as a PostgreSQL text type