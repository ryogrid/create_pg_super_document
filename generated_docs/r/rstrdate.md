# rstrdate

## Location
src/interfaces/ecpg/compatlib/informix.c: 529 - 534

## Overview
Converts a string in mm/dd/yyyy format to a date value, providing Informix compatibility functionality in PostgreSQL's ECPG interface.

## Definition
```c
int rstrdate(const char *str, date *d)
```

## Detailed Description
The `rstrdate` function is part of PostgreSQL's ECPG date handling compatibility library for Informix. It provides a convenient wrapper for parsing date strings in the standard mm/dd/yyyy format. The function internally calls `rdefmtdate` with a fixed format string "mm/dd/yyyy", allowing applications to parse dates without having to specify the format explicitly. Any non-numeric character can be used as a separator between the month, day, and year components, providing flexibility in input formatting while maintaining the expected order.

## Parameters / Member Variables
- `str`: Pointer to the input string containing the date in mm/dd/yyyy format (with flexible separators)
- `d`: Pointer to the date variable where the parsed date value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - rdefmtdate
  - date (type)
- Called from (representative examples):
  - date_test_strdate (in test programs)
  - ECPG_INFORMIX_EXTRA_CHARS (referenced in header)

## Notes and Other Information
- Simple wrapper around `rdefmtdate` with a hardcoded "mm/dd/yyyy" format
- Accepts flexible separators (any non-numeric character) between date components
- Returns the same error codes as `rdefmtdate` (0 on success, error codes on failure)
- Part of the Informix compatibility layer in PostgreSQL ECPG
- Located in src/interfaces/ecpg/compatlib/informix.c:529-534
- Companion function to `rdatestr` (converts date to string vs. string to date)
- Assumes US date format (month/day/year) rather than international formats