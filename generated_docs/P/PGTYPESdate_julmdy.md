# PGTYPESdate_julmdy

## Location
[src/interfaces/ecpg/pgtypeslib/datetime.c:115-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/datetime.c#L115-L127)

## Overview
Converts a Julian date to month/day/year format and stores the values in an integer array.

## Definition
```c
void PGTYPESdate_julmdy(date jd, int *mdy)
```

## Detailed Description
This function takes a Julian date value and converts it to the conventional month/day/year format. The function uses PostgreSQL's internal date conversion routines (j2date and date2j) to perform the conversion. The Julian date is first adjusted by adding the Julian day number for January 1, 2000, then converted to year/month/day components, which are finally rearranged into the month/day/year order expected by the output array.

## Parameters / Member Variables
- `jd`: Julian date value to be converted
- `mdy`: Integer array to store the converted date components where:

## Dependencies
- Functions called/Symbols referenced:
  - [date2j](../d/date2j.md): Converts year/month/day to Julian day number
  - [j2date](../j/j2date.md): Converts Julian day number to year/month/day
- Called from (representative examples):
  - [rjulmdy](../r/rjulmdy.md): Informix compatibility wrapper function
  - [main](../m/main.md): Used in test programs (dt_test.c)

## Notes and Other Information
- This function is part of PostgreSQL's ECPG (Embedded SQL in C) pgtypes library
- The function modifies the provided integer array in-place
- Uses a reference date of January 1, 2000 for Julian date calculations
- Part of the date manipulation utilities for embedded SQL applications