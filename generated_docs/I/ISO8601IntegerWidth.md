# ISO8601IntegerWidth

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:81-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L81-L93)

## Overview
ISO8601IntegerWidth determines the number of integral digits in a valid ISO 8601 number field, ignoring any sign and fractional parts.

## Definition

```c
static int
ISO8601IntegerWidth(const char *fieldstart)
```
## Detailed Description
This utility function is designed to analyze ISO 8601 formatted number fields and count only the integral digit portion. It's particularly useful in ISO 8601 interval parsing where the width of the integer part of a number field affects interpretation according to the ISO 8601 specification. The function handles negative numbers by skipping over a leading minus sign and then counting consecutive decimal digits.

The function implements a simple two-step process:
1. Skip over any leading minus sign to handle negative numbers
2. Use strspn() to count consecutive decimal digits from the current position

This is used in ISO 8601 parsing contexts where the number of digits in the integer portion of a field has semantic meaning for format validation and interpretation.

## Parameters / Member Variables
- : Pointer to the start of the number field string to analyze

## Dependencies
- Functions called/Symbols referenced:
  - strspn (standard C library function to count characters from a set)
- Called from (representative examples):
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md) (multiple locations in backend and ECPG)

## Notes and Other Information
- This is a static helper function within src/backend/utils/adt/datetime.c
- Returns an integer count of decimal digits in the integer portion
- Does not validate the number format - assumes input is a valid number field
- Specifically designed for ISO 8601 compliance where digit count matters
- The function only counts the '0'-'9' characters, stopping at any non-digit
- Does not handle or count fractional parts (digits after decimal point)
- There is also an ECPG version in src/interfaces/ecpg/pgtypeslib/interval.c with identical functionality
- Part of the broader ISO 8601 interval parsing infrastructure in PostgreSQL
- Used primarily for format validation and interpretation in ISO 8601 interval strings