# timestamptz_gt_date

## Location
[src/backend/utils/adt/date.c:997-1005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L997-L1005)

## Overview
A PostgreSQL function that compares a timestamp with timezone to a date value to determine if the timestamp is greater than the date.

## Definition


## Detailed Description
This function implements the greater-than comparison operator (>) between a timestamptz (timestamp with timezone) and a date value. It extracts the timestamptz and date arguments from the function call context, then uses the internal comparison function  to perform the comparison. The function returns true if the timestamp is greater than the date (i.e., the comparison function returns < 0), false otherwise.

## Parameters / Member Variables
- : Standard PostgreSQL function argument context containing:
  - Argument 0:  - The timestamp with timezone value to compare
  - Argument 1:  - The date value to compare against

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMPTZ - Extracts timestamptz argument
  - PG_GETARG_DATEADT - Extracts date argument
  - [date_cmp_timestamptz_internal](../d/date_cmp_timestamptz_internal.md) - Performs the actual comparison logic
  - DateADT - Date abstract data type
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL operator dispatch)

## Notes and Other Information
- This function is part of PostgreSQL's date/time operator infrastructure
- The actual comparison logic is delegated to 
- Returns a PostgreSQL Datum boolean value using PG_RETURN_BOOL macro
- Used internally by the PostgreSQL executor when processing '>' operators between timestamptz and date types
- The comparison logic checks if  to determine if timestamptz > date
- Location: src/backend/utils/adt/date.c:997-1005