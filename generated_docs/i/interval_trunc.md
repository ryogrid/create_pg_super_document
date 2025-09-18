# interval_trunc

## Location
[src/backend/utils/adt/timestamp.c:5017-5115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5017-L5115)

## Overview
Truncates an interval value to specified units by zeroing out all more precise components while preserving larger units.

## Definition


## Detailed Description
This function truncates an interval to the specified time unit precision. It converts the interval to an internal time structure (pg_itm), then systematically zeros out all time components that are more precise than the specified unit. For example, truncating to 'hour' will zero out minutes, seconds, and microseconds while preserving years, months, days, and hours. The function uses a cascading switch statement with fall-through behavior to implement the truncation logic efficiently. Special handling is provided for units like quarter (rounds to nearest 3-month boundary) and millisecond precision.

## Parameters / Member Variables
-  (text): The time unit to truncate to (e.g., 'millennium', 'century', 'decade', 'year', 'quarter', 'month', 'day', 'hour', 'minute', 'second', 'millisec', 'microsec')
-  (Interval*): The interval value to be truncated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (for extracting text argument)
  - PG_GETARG_INTERVAL_P (for extracting interval argument) 
  - [palloc](../p/palloc.md) (for memory allocation)
  - INTERVAL_NOT_FINITE (macro for checking infinite intervals)
  - memcpy (for copying infinite interval values)
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md) (for normalizing unit names)
  - [DecodeUnits](../D/DecodeUnits.md) (for parsing time unit strings)
  - [interval2itm](interval2itm.md) (for converting interval to internal time structure)
  - [itm2interval](itm2interval.md) (for converting back to interval)
  - ereport/errcode/errmsg (for error reporting)
  - [format_type_be](../f/format_type_be.md) (for formatting type names in errors)
  - PG_RETURN_INTERVAL_P (for returning result)
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- Handles infinite interval values by returning them unchanged
- Uses cascading switch statement with fall-through to implement efficient truncation
- Special handling for quarters (truncates to 3-month boundaries)
- Week truncation is explicitly not supported due to the complexity of fractional weeks in months
- Millisecond truncation preserves microsecond precision in thousands
- Includes comprehensive error handling for unsupported or unrecognized units
- The function carefully handles C division behavior for negative remainders when truncating larger units
- Located in src/backend/utils/adt/timestamp.c:5017-5115