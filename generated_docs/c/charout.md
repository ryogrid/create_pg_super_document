# charout

## Location
[src/backend/utils/adt/char.c:64-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/char.c#L64-L93)

## Overview
Converts a single character value to its string representation, formatting high-bit characters as octal escape sequences.

## Definition


## Detailed Description
The charout function is the output function for PostgreSQL's "char" (single character) data type. It converts a character value to its string representation following specific formatting rules:

1. **Null character (0x00)**: Represented as an empty string
2. **ASCII characters (0x01-0x7F)**: Represented as a single ASCII byte
3. **High-bit characters (0x80-0xFF)**: Represented as octal escape sequences in the format \ooo (backslash followed by 3 octal digits)

The octal escape format for high-bit characters matches the traditional "escape" format used by PostgreSQL's bytea data type, ensuring consistency across the type system.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Character value to be converted to string representation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CHAR (to extract input character)
  - [palloc](../p/palloc.md) (to allocate memory for result string)
  - IS_HIGHBIT_SET (macro to check if character has high bit set)
  - TOOCTAL (macro to convert numeric value to octal character)
  - PG_RETURN_CSTRING (to return the string result)
- Called from (representative examples):
  - PostgreSQL type system during output formatting
  - SQL queries when casting "char" type to text

## Notes and Other Information
- The function allocates exactly 5 bytes for the result: up to 4 characters plus null terminator
- For high-bit characters, the octal representation is calculated by:
  - First octal digit: (ch >> 6) & 7
  - Second octal digit: (ch >> 3) & 7  
  - Third octal digit: ch & 7
- The TOOCTAL macro simply adds '0' to convert a numeric octal digit to its character representation
- IS_HIGHBIT_SET checks if the most significant bit of the character is set (>= 0x80)
- Output format is designed to be compatible with charin function for round-trip conversion
- The function handles the null character (0x00) by producing a single-byte string containing only the null terminator