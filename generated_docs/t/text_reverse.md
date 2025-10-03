# text_reverse

## Location
[src/backend/utils/adt/varlena.c:5583-5622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5583-L5622)

## Overview
The  function reverses the character order of a text string while properly handling multibyte character encodings.

## Definition

```c
Datum
text_reverse(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements PostgreSQL's  SQL function for text data types. It creates a new text object containing the input string with its characters in reverse order. The implementation includes optimizations for both single-byte and multibyte character encodings:

- For multibyte encodings: Uses  to determine the length of each multibyte character and copies complete characters as units
- For single-byte encodings: Uses a simpler byte-by-byte reversal for better performance

The function allocates a new text object of the same size as the input and fills it from right to left with the characters from the original string.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The input text string to reverse (retrieved via )
## Dependencies
- Functions called/Symbols referenced:
  -  - Extract text argument from function call
  -  - Get pointer to variable-length data
  -  - Get size of variable-length data excluding header
  -  - Allocate memory in current memory context
  -  - Get pointer to variable-length data area
  -  - Set size of variable-length data
  -  - Check if database uses multibyte encoding
  -  - Get length of multibyte character
  -  - Copy memory blocks
  -  - Return text value from function
- Called from (representative examples):
  - SQL REVERSE() function invocations

## Notes and Other Information
- Located in 
- Handles both single-byte and multibyte character encodings correctly
- Uses character-aware reversal rather than simple byte reversal for multibyte strings
- Allocates new memory for the result rather than modifying the input in-place
- The destination pointer starts at the end of the allocated space and works backward
- Performance optimized with separate code paths for single-byte vs multibyte encodings