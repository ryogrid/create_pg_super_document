# timestamptz_date

## Location
[src/backend/utils/adt/date.c:1342-1373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1342-L1373)

## Overview
Converts a PostgreSQL timestamp with time zone (timestamptz) value to a date data type, discarding the time and timezone components while preserving the date portion.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function is a PostgreSQL built-in function that converts a timestamp with time zone value (TimestampTz) to a date value (DateADT). This conversion extracts only the date portion from the timestamptz after applying timezone conversion, effectively discarding both the time component and timezone information. The function handles special timestamp values like infinity (NOBEGIN/NOEND) by converting them to corresponding special date values.

The conversion process involves breaking down the timestamptz into its constituent parts using  with timezone consideration, then reconstructing the date using the Julian day calculation via . The timezone offset is applied during the timestamp2tm conversion to ensure the correct local date is extracted. Error checking is performed to ensure the timestamp is within valid range.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure
  - Argument 0:  - The input timestamp with timezone value to be converted

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to extract timestamp argument
  -  - Macro to check for negative infinity
  -  - Macro to check for positive infinity
  -  - Macro to set date to negative infinity
  -  - Macro to set date to positive infinity
  -  - Function to break timestamp into time components with timezone
  -  - Function to convert date to Julian day number
  -  - Macro to return date result
  -  - Error reporting function
- Types used:
  -  - PostgreSQL timestamp with timezone type
  -  - PostgreSQL date type
  -  - Time structure
  -  - Fractional seconds type
  -  - Timezone offset variable
  -  - PostgreSQL generic return type
- Constants used:
  -  - PostgreSQL epoch reference point
- Called from (representative examples):
  -  (in jsonpath execution)

## Notes and Other Information
- This function is part of PostgreSQL's date/time type conversion system
- The time component and timezone information are completely discarded during conversion
- Timezone conversion is applied before extracting the date to ensure correct local date
- Special handling for infinite timestamp values (NOBEGIN/NOEND) maintains their special meaning in date form
- Error checking ensures timestamps are within valid range before conversion
- Located in 
- The conversion uses Julian day calculations to maintain date accuracy
- Unlike , this function considers timezone when determining the resulting date
- Used primarily in SQL contexts where implicit or explicit conversion from timestamptz to date is needed
- The resulting date represents the date portion in the timestamp's timezone context