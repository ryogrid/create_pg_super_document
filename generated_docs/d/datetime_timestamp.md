# datetime_timestamp

## Location
[src/backend/utils/adt/date.c:1966-1988](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1966-L1988)

## Overview
Combines a date value and a time value to create a timestamp, merging separate date and time components into a single timestamp data type.

## Definition

```c
Datum
datetime_timestamp(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that takes two separate inputs - a date (DateADT) and a time (TimeADT) - and combines them to produce a complete timestamp. The function first converts the date component to a timestamp representation, then adds the time component (represented as microseconds since midnight) to create the final timestamp value.

The function performs the following operations:
1. Extracts the date and time arguments from the function parameters
2. Converts the date to a timestamp using 
3. For finite timestamps, adds the time component to the date-based timestamp
4. Validates that the resulting timestamp is within valid range
5. Returns the combined timestamp result

This function is essential for reconstructing complete datetime information from separate date and time components.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - Sun Sep 14 18:43:00 JST 2025 (DateADT): The date component to be combined
  -  (TimeADT): The time component to be combined

## Dependencies
- Functions called/Symbols referenced:
  - DateADT: Date abstract data type for storing date values
  - PG_GETARG_DATEADT: Macro to extract DateADT argument
  - TimeADT: Time abstract data type for storing time values
  - PG_GETARG_TIMEADT: Macro to extract TimeADT argument
  - Timestamp: Timestamp data type for the result
  - [date2timestamp](date2timestamp.md): Function to convert date to timestamp
  - TIMESTAMP_NOT_FINITE: Macro to check for infinite timestamp values
  - IS_VALID_TIMESTAMP: Macro to validate timestamp range
  - PG_RETURN_TIMESTAMP: Macro to return timestamp result

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- The function preserves infinite date values (infinity/-infinity) without adding time components
- [Range](../R/Range.md) validation ensures the combined result doesn't overflow timestamp limits
- The time component is added as microseconds to the base timestamp from the date
- Error handling includes validation of the final timestamp range
- The function assumes the time component represents local time (no timezone handling)
- Located in src/backend/utils/adt/date.c:1966-1988