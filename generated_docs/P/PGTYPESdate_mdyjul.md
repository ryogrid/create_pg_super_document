# PGTYPESdate_mdyjul

## Location
[src/interfaces/ecpg/pgtypeslib/datetime.c:128-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/datetime.c#L128-L137)

## Overview
Converts month/day/year format to a Julian date value by calculating the difference from a reference date.

## Definition
```c
void PGTYPESdate_mdyjul(int *mdy, date *jdate)
```

## Detailed Description
This function performs the inverse operation of PGTYPESdate_julmdy by converting month/day/year components stored in an integer array to a Julian date. The function uses PostgreSQL's date2j routine to convert the given date components to a Julian day number, then subtracts the Julian day number for January 1, 2000 to produce a relative Julian date value that matches PostgreSQL's internal date representation.

## Parameters / Member Variables
- `mdy`: Integer array containing date components where:
  - mdy[0] = month (1-12)
  - mdy[1] = day (1-31)
  - mdy[2] = year
- `jdate`: Pointer to date variable where the calculated Julian date will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [date2j](../d/date2j.md): Converts year/month/day to Julian day number (called twice)
- Called from (representative examples):
  - [rmdyjul](../r/rmdyjul.md): Informix compatibility wrapper function
  - [main](../m/main.md): Used in test programs (dt_test.c)

## Notes and Other Information
- This function is the complement to PGTYPESdate_julmdy, performing the reverse conversion
- Part of PostgreSQL's ECPG (Embedded SQL in C) pgtypes library
- Uses January 1, 2000 as the reference point for Julian date calculations
- The calculated Julian date is relative to PostgreSQL's epoch, not absolute Julian day numbers
- Essential for converting user-friendly date formats to PostgreSQL's internal representation