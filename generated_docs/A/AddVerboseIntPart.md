# AddVerboseIntPart

## Location
[src/backend/utils/adt/datetime.c:4546-4584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4546-L4584)

## Overview
A static helper function that appends a verbose-style interval field to a string buffer, but only if the value is non-zero.

## Definition
```c
static char *AddVerboseIntPart(char *cp, int64 value, const char *units, bool *is_zero, bool *is_before)
```

## Detailed Description
This function formats individual components of PostgreSQL interval values in verbose output format. Unlike the PostgreSQL-style format, the verbose format handles sign representation differently - the first non-zero field determines the overall sign direction, and subsequent fields follow that sign without individual sign indicators. The function ensures proper spacing, absolute value handling for consistent sign representation, and pluralization of unit names.

## Parameters / Member Variables
- `cp`: Pointer to the current position in the output string buffer where the formatted interval part should be appended
- `value`: The numeric value of the interval component (e.g., number of years, months, days)
- `units`: The unit name string (e.g., "year", "month", "day")
- `is_zero`: Pointer to a boolean flag indicating whether any non-zero values have been encountered yet
- `is_before`: Pointer to a boolean flag that tracks the overall sign direction for the entire interval

## Dependencies
- Functions called/Symbols referenced:
  - i64abs (PostgreSQL utility function for 64-bit absolute value)
  - sprintf (standard C library function)
  - strlen (standard C library function)
- Called from (representative examples):
  - [EncodeInterval](../E/EncodeInterval.md) (in src/backend/utils/adt/datetime.c)
  - [EncodeInterval](../E/EncodeInterval.md) (in src/interfaces/ecpg/pgtypeslib/interval.c)

## Notes and Other Information
- Only appends content when the value is non-zero, creating clean verbose interval representations
- The first non-zero field determines the sign for the entire interval representation
- Uses absolute values internally but applies the determined sign consistently across all fields
- Handles pluralization by omitting 's' for singular values and appending 's' for plural values
- Part of PostgreSQL's interval data type formatting system, specifically for verbose output style
- Always prefixes output with a space for consistent formatting