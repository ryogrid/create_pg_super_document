# timestamp_date

## Location
[src/backend/utils/adt/date.c:1297-1326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1297-L1326)

## Overview
Converts a PostgreSQL timestamp value to a date data type, discarding the time component and keeping only the date portion.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function is a PostgreSQL built-in function that converts a timestamp value (Timestamp) to a date value (DateADT). This conversion extracts only the date portion from the timestamp, effectively discarding the time component. The function handles special timestamp values like infinity (NOBEGIN/NOEND) by converting them to corresponding special date values.

The conversion process involves breaking down the timestamp into its constituent parts using , then reconstructing the date using the Julian day calculation via . Error checking is performed to ensure the timestamp is within valid range.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure
  - Argument 0:  - The input timestamp value to be converted

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to extract timestamp argument
  -  - Macro to check for negative infinity
  -  - Macro to check for positive infinity
  -  - Macro to set date to negative infinity
  -  - Macro to set date to positive infinity
  -  - Function to break timestamp into time components
  -  - Function to convert date to Julian day number
  -  - Macro to return date result
  -  - Error reporting function
- Types used:
  -  - PostgreSQL timestamp type
  -  - PostgreSQL date type
  -  - Time structure
  -  - Fractional seconds type
  -  - PostgreSQL generic return type
- Constants used:
  -  - PostgreSQL epoch reference point
- Called from (representative examples):
  -  (in jsonpath execution)

## Notes and Other Information
- This function is part of PostgreSQL's date/time type conversion system
- The time component of the timestamp is completely discarded during conversion
- Special handling for infinite timestamp values (NOBEGIN/NOEND) maintains their special meaning in date form
- Error checking ensures timestamps are within valid range before conversion
- Located in 
- The conversion uses Julian day calculations to maintain date accuracy
- Used primarily in SQL contexts where implicit or explicit conversion from timestamp to date is needed

## Simplified Source

```c
Datum
timestamp_date(PG_FUNCTION_ARGS)
{
    Timestamp timestamp = PG_GETARG_TIMESTAMP(0);
    DateADT result;
    struct pg_tm time_components;
    fsec_t fractional_seconds;

    // Handle special infinite values
    if (TIMESTAMP_IS_NOBEGIN(timestamp))
        DATE_NOBEGIN(result);
    else if (TIMESTAMP_IS_NOEND(timestamp))
        DATE_NOEND(result);
    else
    {
        // Convert timestamp to time components
        if (timestamp2tm(timestamp, NULL, &time_components, &fractional_seconds, NULL, NULL) != 0)
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("timestamp out of range")));

        // Convert date components to Julian day number, then to PostgreSQL date
        result = date2j(time_components.tm_year, time_components.tm_mon, time_components.tm_mday) - POSTGRES_EPOCH_JDATE;
    }

    PG_RETURN_DATEADT(result);
}
```