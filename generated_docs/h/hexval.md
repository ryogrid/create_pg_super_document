# hexval

## Location
[src/backend/utils/adt/varlena.c:6472-6487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6472-L6487)

## Overview
Converts a single hexadecimal digit character to its corresponding numeric value (0-15).

## Definition
```c
static unsigned int hexval(unsigned char c)
```

## Detailed Description
The `hexval` function is a static utility function that converts a single hexadecimal digit character to its corresponding numeric value. It accepts characters '0'-'9' (returning values 0-9), 'a'-'f' (returning values 10-15), and 'A'-'F' (returning values 10-15). The function assumes that the caller has already verified that the input character is a valid hexadecimal digit. If an invalid character is passed, the function raises an ERROR using elog() and returns 0 (though this return statement is never reached due to the error). This function is commonly used in parsing operations where hexadecimal sequences need to be converted to their numeric equivalents.

## Parameters / Member Variables
- `c`: An `unsigned char` representing the hexadecimal digit character to be converted

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
- Called from:
  - [str_udeescape](../s/str_udeescape.md) (multiple references at lines 425-428, 462-467)
  - [hexval_n](hexval_n.md) (at src/backend/utils/adt/varlena.c:6493)

## Notes and Other Information
- This is a static function with internal linkage, accessible only within the parser.c translation unit
- The caller is responsible for validating that the input character is a valid hexadecimal digit
- The function handles both uppercase and lowercase hexadecimal letters
- The error case should never be reached if the function is used correctly
- Used primarily in string parsing operations, particularly for Unicode escape sequence processing
- Returns values in the range 0-15 corresponding to hexadecimal digits 0-F

## Simplified Source

```c
static unsigned int hexval(unsigned char c) {
    // Convert '0'-'9' to 0-9
    if (c >= '0' && c <= '9')
        return c - '0';

    // Convert 'a'-'f' to 10-15
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 0xA;

    // Convert 'A'-'F' to 10-15
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 0xA;

    // Should never reach here if caller validates input
    elog(ERROR, "invalid hexadecimal digit");
    return 0;
}
```