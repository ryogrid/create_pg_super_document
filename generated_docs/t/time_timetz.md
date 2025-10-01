# time_timetz

## Location
[src/backend/utils/adt/date.c:2828-2853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2828-L2853)

## Overview
Converts a plain time (TimeADT) value to a time with time zone (TimeTzADT) by adding the current session's timezone information.

## Definition
```c
Datum time_timetz(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs a conversion from a plain time data type to a time with time zone data type. It takes a TimeADT input parameter and constructs a TimeTzADT result by preserving the original time value and determining the appropriate timezone offset based on the current session timezone. The function uses the current date/time context to determine the timezone offset, which is necessary because timezone offsets can vary due to daylight saving time rules. The conversion process involves getting the current datetime, converting the time to a broken-down time structure, and then determining the timezone offset for the session timezone.

## Parameters / Member Variables
- Input parameter (via PG_GETARG_TIMEADT(0)): A TimeADT value representing the plain time to convert

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMEADT: Macro to extract TimeADT argument from function call
  - [GetCurrentDateTime](../G/GetCurrentDateTime.md): Function to get current date/time information
  - [time2tm](time2tm.md): Function to convert TimeADT to broken-down time structure
  - [DetermineTimeZoneOffset](../D/DetermineTimeZoneOffset.md): Function to calculate timezone offset for given time and timezone
  - [palloc](../p/palloc.md): Memory allocation function
  - PG_RETURN_TIMETZADT_P: Macro to return TimeTzADT result
- Types used:
  - TimeADT: Plain time data type
  - TimeTzADT: Time with timezone data type
  - [pg_tm](../p/pg_tm.md): Broken-down time structure
  - fsec_t: Fractional seconds type
- Global variables accessed:
  - session_timezone: Current session's timezone setting
- Called from (representative examples):
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md): Used in JSON path execution for datetime method processing
  - [castTimeToTimeTz](../c/castTimeToTimeTz.md): Used for casting time to time with timezone in JSON path operations

## Notes and Other Information
- The function allocates memory for the result TimeTzADT structure using palloc
- The timezone offset determination depends on the current session timezone setting
- The conversion preserves the exact time value while adding timezone context
- Located in src/backend/utils/adt/date.c alongside other date/time conversion functions
- The function follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS
- Timezone offset calculation takes into account daylight saving time rules when applicable

## Simplified Source

```c
Datum time_timetz(PG_FUNCTION_ARGS) {
    TimeADT time = PG_GETARG_TIMEADT(0);
    TimeTzADT *result;
    struct pg_tm tt, *tm = &tt;
    fsec_t fsec;
    int tz;

    // Get current datetime context for timezone calculation
    GetCurrentDateTime(tm);
    time2tm(time, tm, &fsec);

    // Determine timezone offset based on session timezone
    tz = DetermineTimeZoneOffset(tm, session_timezone);

    // Create result with original time plus timezone offset
    result = (TimeTzADT *) palloc(sizeof(TimeTzADT));
    result->time = time;
    result->zone = tz;

    PG_RETURN_TIMETZADT_P(result);
}
```