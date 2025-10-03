# GetDecimalFromHex

## Location
[src/backend/commands/copyfromparse.c:1509-1536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L1509-L1536)

## Overview
GetDecimalFromHex converts a single hexadecimal character ('0'-'9', 'a'-'f', 'A'-'F') to its corresponding decimal integer value (0-15).

## Definition

```c
static int
GetDecimalFromHex(char hex)
```
## Detailed Description
This simple utility function performs hexadecimal to decimal character conversion for use in parsing escape sequences during COPY FROM text processing. It handles both numeric digits ('0'-'9') and alphabetic hex digits ('a'-'f', 'A'-'F'), converting them to their decimal equivalents in the range 0-15. The function uses a case-insensitive approach by converting alphabetic characters to lowercase before performing the conversion calculation.

For numeric characters, it subtracts the ASCII value of '0' to get the decimal value. For alphabetic characters, it first converts to lowercase, then subtracts the ASCII value of 'a' and adds 10 to get the proper decimal value in the range 10-15. This function is typically used as a helper for processing hexadecimal escape sequences in text input parsing.

## Parameters / Member Variables
- `hex`: A single hexadecimal character that should be converted to its decimal equivalent
## Dependencies
- Functions called/Symbols referenced:
  - isdigit: Standard library function to check if character is a decimal digit
  - tolower: Standard library function to convert character to lowercase
- Called from (representative examples):
  - [CopyReadAttributesText](../C/CopyReadAttributesText.md): Uses this function when processing hexadecimal escape sequences in text mode parsing

## Notes and Other Information
- Static function - only accessible within the copyfromparse.c module
- Assumes input is a valid hexadecimal character - no validation is performed
- Handles both uppercase and lowercase alphabetic hex digits (A-F, a-f)
- Returns values in range 0-15 for valid hex characters
- Used specifically for parsing \x escape sequences in COPY FROM text input
- Does not perform error checking - calling code must validate hex character validity
- Efficient implementation using simple arithmetic operations rather than lookup tables

## Simplified Source

```c
static int
GetDecimalFromHex(char hex)
{
    // Convert hex digit to decimal value (0-15)
    if (isdigit((unsigned char) hex))
        return hex - '0';           // '0'-'9' -> 0-9
    else
        return tolower((unsigned char) hex) - 'a' + 10;  // 'a'-'f'/'A'-'F' -> 10-15
}
```