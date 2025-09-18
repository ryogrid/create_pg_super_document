# timetz_part

## Location
[src/backend/utils/adt/date.c:3044-3049](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L3044-L3049)

## Overview
Extracts specified field from a time with time zone (TIMETZ) data type, returning the result as a float8 value.

## Definition


## Detailed Description
 is a PostgreSQL built-in function that extracts a specific field (hour, minute, second, timezone, etc.) from a time with time zone value. It serves as a wrapper function that calls  with , ensuring the result is returned as a floating-point number rather than a numeric type. This function is typically used in SQL expressions like  or through the  function.

The function handles various time components including hours, minutes, seconds (with fractional parts), microseconds, milliseconds, and timezone-related fields. It converts the internal TimeTzADT representation to a broken-down time structure for field extraction.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (text): The field name to extract (e.g., 'hour', 'minute', 'second', 'timezone')
  -  (TimeTzADT*): The input time with time zone value

## Dependencies
- Functions called/Symbols referenced:
  - [timetz_part_common](timetz_part_common.md)
  - PG_GETARG_TEXT_PP
  - PG_GETARG_TIMETZADT_P
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md)
  - [DecodeUnits](../D/DecodeUnits.md)
  - [DecodeSpecial](../D/DecodeSpecial.md)
  - [timetz2tm](timetz2tm.md)
  - PG_RETURN_FLOAT8
  - ereport
- Called from (representative examples):
  - SQL EXTRACT() expressions for TIMETZ types
  - date_part() function calls

## Notes and Other Information
- This function only supports time-related fields; date fields (day, month, year) will trigger an error
- Timezone values are returned as seconds offset from UTC, with sign inverted (positive for west of UTC)
- Fractional seconds are supported through microsecond and millisecond extraction
- The function uses the TIMETZ internal representation which stores time as microseconds since midnight and timezone as seconds offset
- Error handling includes proper reporting for unsupported units and invalid parameter values