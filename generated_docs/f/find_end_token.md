# find_end_token

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:2352-2456](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L2352-L2456)

## Overview
A static helper function that locates the end position of a token in a string during date/time format string parsing in ECPG (Embedded SQL in C for PostgreSQL).

## Definition

```c
static char *
find_end_token(char *str, char *fmt)
```
## Detailed Description
This function is a critical component of the ECPG date/time parsing system. It analyzes a format string pattern and finds where the current token ends in the input string by looking for the next literal text delimiter. The function handles the complex task of matching format specifiers with actual date/time values while accounting for flexible whitespace handling.

The function works by:
1. Finding the next percent sign (%) in the format string that indicates the start of the next format specifier
2. Extracting the literal text between the current position and that next format specifier
3. Searching for that literal text in the input string to determine where the current token ends
4. Handling special cases like trailing whitespace and end-of-string conditions

For example, given str="28the day12" and fmt="the day%h", it finds "the day" as the delimiter and returns a pointer to where "the day" starts in str, indicating that "28" is the complete token.

## Parameters / Member Variables
- `*str`: Input string containing the date/time value being parsed
- `*fmt`: Format string pattern that defines the expected structure
## Dependencies
- Functions called/Symbols referenced:
  - strchr (standard C library function)
  - strstr (standard C library function)
  - strlen (standard C library function)
- Called from (representative examples):
  - [pgtypes_defmt_scan](../p/pgtypes_defmt_scan.md)

## Notes and Other Information
- This is a static function, only accessible within the dt_common.c file
- Handles dynamic whitespace padding by skipping leading spaces in the input string
- Uses temporary null termination to isolate pattern segments for strstr matching
- Contains special case handling for space-only delimiters at the end of patterns
- Part of the ECPG pgtypes library for date/time manipulation
- Located in src/interfaces/ecpg/pgtypeslib/dt_common.c:2352-2456