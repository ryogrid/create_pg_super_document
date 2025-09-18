# pg_get_timezone_offset

## Location
[src/timezone/localtime.c:1851-1874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1851-L1874)

## Overview
This function determines if a timezone uses only one GMT offset and retrieves that offset value if so.

## Definition


## Detailed Description
pg_get_timezone_offset checks whether the given timezone has a consistent GMT offset across all its timezone transition information (ttinfo) entries. A timezone may have multiple ttinfo entries if it has historically used more than one abbreviation, but this function returns true only if all entries share the same GMT offset.

The function iterates through all timezone type entries in the timezone state and compares their UTC offsets. If any offset differs from the first one, the function returns false. If all offsets are identical, it sets the output parameter to that offset and returns true.

## Parameters / Member Variables
- : The timezone structure to examine for offset consistency
- : Output parameter that receives the GMT offset in seconds if the timezone uses only one offset

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tz](pg_tz.md) (timezone structure type)
  - struct state (internal timezone state structure)
  - [ttinfo](../t/ttinfo.md) (timezone transition info structure via sp->ttis array)
- Called from (representative examples):
  - [DecodeTimeOnly](../D/DecodeTimeOnly.md) (src/backend/utils/adt/datetime.c:2309)
  - [TimestampTimestampTzRequiresRewrite](../T/TimestampTimestampTzRequiresRewrite.md) (src/backend/utils/adt/timestamp.c:6277)

## Notes and Other Information
- Returns true if the timezone uses a single GMT offset, false if it uses multiple offsets
- When returning true, the gmtoff parameter is set to the consistent offset value
- Useful for determining if a timezone can be treated as having a fixed offset
- The function examines the ttis array in the timezone's state structure
- Compares tt_utoff (UTC offset) field of each ttinfo entry
- Located in src/timezone/localtime.c:1851-1874