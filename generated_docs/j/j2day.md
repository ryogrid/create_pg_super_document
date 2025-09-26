# j2day

## Location
src/backend/utils/adt/datetime.c: 344 - 365

## Overview
Converts a Julian day number to the corresponding day of the week (0..6 representing Sunday..Saturday).

## Definition
```c
int j2day(int date)
```

## Detailed Description
The `j2day` function takes a Julian day number and returns the corresponding day of the week using a simple modular arithmetic approach. The function follows the convention where 0 represents Sunday and 6 represents Saturday (0..6 == Sun..Sat).

The algorithm works by adding 1 to the input date (to align with the Julian day epoch), taking modulo 7, and handling the case where division might truncate towards zero on some systems. This ensures the result is always in the range 0-6 regardless of the platform's division behavior.

The function includes a notable implementation detail: various places in the codebase use the pattern `j2day(date - 1)` to produce results according to the Monday..Sunday convention (0..6 = Mon..Sun), which works correctly as long as the computation remains a simple modulo operation.

## Parameters / Member Variables
- `date`: The Julian day number for which to determine the day of the week

## Dependencies
- Functions called/Symbols referenced:
  - No external function calls (uses only basic arithmetic operations)
- Called from (representative examples):
  - extract_date (date extraction operations for DOW field)
  - EncodeDateTime (datetime formatting that includes day names)
  - isoweek2j (ISO week calculations)
  - date2isoweek (ISO week number calculations)
  - date2isoyear (ISO year calculations)
  - timestamp_part_common (timestamp part extraction)
  - timestamptz_part_common (timestamptz part extraction)

## Notes and Other Information
- The function uses the standard astronomical/programming convention where Sunday = 0, Monday = 1, ..., Saturday = 6
- Some callers use `j2day(date - 1)` to get Monday-based numbering (Mon=0, Tue=1, ..., Sun=6)
- The algorithm handles negative Julian day numbers correctly by ensuring the result is always non-negative through the conditional addition of 7
- This is a fundamental utility function used throughout PostgreSQL's date/time system for day-of-week calculations
- The implementation is deliberately simple and efficient, using only basic arithmetic operations
- Critical for ISO week calculations and date formatting operations that need to know the day of the week
- The function handles the mathematical relationship between Julian day numbers and the 7-day week cycle