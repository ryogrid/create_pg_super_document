# PGTYPESdate_dayofweek

## Location
src/interfaces/ecpg/pgtypeslib/datetime.c: 138 - 147

## Overview
Calculates the day of the week for a given Julian date, returning an integer from 0 (Sunday) to 6 (Saturday).

## Definition
```c
int PGTYPESdate_dayofweek(date dDate)
```

## Detailed Description
This function determines the day of the week for a given Julian date by converting it to an absolute Julian day number and applying modular arithmetic. The function adds the Julian day number for January 1, 2000 to convert the relative date to an absolute Julian day, then adds 1 to align with the desired weekday numbering system and uses modulo 7 to get the final day-of-week value.

## Parameters / Member Variables
- `dDate`: Julian date value to determine the day of week for

## Dependencies
- Functions called/Symbols referenced:
  - date2j: Converts year/month/day to Julian day number (used with reference date 2000-01-01)
- Called from (representative examples):
  - rdayofweek: Informix compatibility wrapper function
  - PGTYPESdate_fmt_asc: Used for date formatting with day names
  - PGTYPEStimestamp_fmt_asc: Used for timestamp formatting with day names
  - main: Used in test programs (dt_test.c)

## Notes and Other Information
- Returns integer values following the convention: Sunday=0, Monday=1, Tuesday=2, Wednesday=3, Thursday=4, Friday=5, Saturday=6
- Part of PostgreSQL's ECPG (Embedded SQL in C) pgtypes library
- Uses January 1, 2000 as the reference date for calculations (January 1, 2000 was a Saturday)
- The +1 adjustment in the formula accounts for the specific alignment needed with the weekday numbering system
- Commonly used by date formatting functions to display day names
- Essential for calendar-based operations and date formatting in embedded SQL applications