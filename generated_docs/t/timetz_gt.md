# timetz_gt

## Location
src/backend/utils/adt/date.c: 2506 - 2514

## Overview
The timetz_gt function compares two time with time zone values and returns true if the first time is greater than (later than) the second time.

## Definition


## Detailed Description
This function implements the greater-than comparison operator for the TimeTzADT (time with time zone) data type. It extracts two TimeTzADT values from the function arguments and uses the internal comparison function timetz_cmp_internal to determine their relative ordering. The function returns true if the first time value is considered greater than the second.

The comparison is performed by first converting both times to GMT-equivalent values (adding the timezone offset) and comparing those. If the GMT times are equal, the comparison falls back to comparing the timezone values themselves to ensure that two timetz values are only considered equal if both their time and zone components are identical.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: First TimeTzADT value (time1)
  - Argument 1: Second TimeTzADT value (time2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P: Extracts TimeTzADT arguments from function call
  - [timetz_cmp_internal](timetz_cmp_internal.md): Internal comparison function that performs the actual comparison logic
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL function call framework
- Data types used:
  - TimeTzADT: Structure containing time (TimeADT) and zone (int32) fields
- Called from (representative examples):
  - SQL greater-than operator (>) for timetz data type

## Notes and Other Information
- This function is part of PostgreSQL's type system infrastructure and is typically called through SQL operators rather than directly
- The comparison logic ensures proper handling of timezone differences by normalizing both times to GMT before comparison
- Returns a PostgreSQL Datum containing a boolean value
- Located in src/backend/utils/adt/date.c:2506-2514