# charin

## Location
[src/backend/utils/adt/char.c:41-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/char.c#L41-L63)

## Overview
Converts a string representation of a character to a single character value, handling both regular character input and octal escape sequences.

## Definition

```c
Datum
charin(PG_FUNCTION_ARGS)
```
## Detailed Description
The charin function is the input function for PostgreSQL's "char" (single character) data type. It accepts string input and converts it to a single character value. The function handles two main input formats:

1. **Octal escape sequences**: Input in the form "\ooo" where each 'o' is an octal digit (0-7). The function converts the three octal digits to their corresponding character value.
2. **Regular character input**: For any other input, including multibyte sequences, it takes only the first byte as the character value and discards the rest for backwards compatibility.

The function specifically accepts the same formats that the charout function produces, ensuring round-trip compatibility for character data serialization.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : C-string input representing the character to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (to extract input string)
  - strlen (to check string length)
  - ISOCTAL (macro to validate octal digits)
  - FROMOCTAL (macro to convert octal character to numeric value)
  - PG_RETURN_CHAR (to return the character result)
- Called from (representative examples):
  - PostgreSQL type system during input parsing
  - SQL queries when casting strings to "char" type

## Notes and Other Information
- The function provides backwards compatibility by accepting multibyte input but only using the first byte
- Octal escape sequences must be exactly 4 characters long (backslash + 3 octal digits)
- For zero-length input strings, the function returns the null character (\0)
- The ISOCTAL macro checks if a character is an octal digit (0-7)
- The FROMOCTAL macro converts an octal character to its numeric equivalent
- This function is part of PostgreSQL's type system infrastructure for the "char" data type

## Simplified Source

```c
Datum charin(PG_FUNCTION_ARGS) {
    // Extract input string
    char *ch = PG_GETARG_CSTRING(0);

    // Check for octal escape sequence format: \ooo
    if (strlen(ch) == 4 && ch[0] == '\\' &&
        ISOCTAL(ch[1]) && ISOCTAL(ch[2]) && ISOCTAL(ch[3])) {
        // Convert octal digits to character value
        PG_RETURN_CHAR((FROMOCTAL(ch[1]) << 6) +
                       (FROMOCTAL(ch[2]) << 3) +
                       FROMOCTAL(ch[3]));
    }

    // For regular input, return first character (handles empty strings too)
    PG_RETURN_CHAR(ch[0]);
}
```