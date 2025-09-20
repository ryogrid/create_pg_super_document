# hexval_n

## Location
[src/backend/utils/adt/varlena.c:6488-6501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6488-L6501)

## Overview
Translates a string containing hexadecimal digits to an unsigned integer value by processing exactly n characters from the input string.

## Definition

```c
static unsigned int
hexval_n(const char *instr, size_t n)
```
## Detailed Description
The  function converts a sequence of hexadecimal characters into their corresponding numeric value. It processes exactly  characters from the input string, treating each character as a hexadecimal digit. The function builds the result by iterating through each character, converting it using the  helper function, and shifting it to the appropriate bit position based on its position in the sequence. The most significant digit comes first in the input string.

The function assumes that all input characters are valid hexadecimal digits - validation should be performed by the caller or will be handled by the underlying  function which will throw an error for invalid digits.

## Parameters / Member Variables
- : Pointer to the input string containing hexadecimal digits
- : Number of hexadecimal characters to process from the input string

## Dependencies
- Functions called/Symbols referenced:
  - : Converts a single hexadecimal character to its numeric value (0-15)
- Called from (representative examples):
  - : Used multiple times to parse Unicode escape sequences in string literals

## Notes and Other Information
- This is a static function local to 
- The function processes characters in big-endian order (most significant digit first)
- No bounds checking is performed on the input string - the caller must ensure  has at least  valid characters
- Used primarily for parsing Unicode escape sequences in the format  and  where X represents hexadecimal digits
- Located at src/backend/utils/adt/varlena.c:6488-6501