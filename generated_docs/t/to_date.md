# to_date

## Location
[src/backend/utils/adt/formatting.c:4407-4454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L4407-L4454)

## Overview
Converts a text string representing a date into PostgreSQL's internal DateADT format using a specified format string.

## Definition

```c
struct pg_tm tm;
```
## Detailed Description
The  function is a PostgreSQL built-in function that parses a date string according to a specified format pattern and returns a DateADT value. It serves as the implementation for the SQL  function. The function uses the  function internally to perform the actual parsing, then converts the resulting timestamp components into a date value by calculating the Julian day number relative to the PostgreSQL epoch.

The function includes validation to ensure the resulting date is within PostgreSQL's supported date range, preventing overflow in Julian-day calculations and ensuring the final date value is valid.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (text*): The input date string to be parsed
  -  (text*): The format string specifying how to interpret the date string
  -  (Oid): The collation ID for string comparison operations

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract text arguments from function call
  -  - Get collation information
  -  - Core date/time parsing function
  -  - Validate Julian date range
  -  - Convert date components to Julian day number
  -  - Validate final date value
  -  - Convert text to C string for error messages
  -  - Return DateADT value
- Called from (representative examples):
  - SQL TO_DATE() function calls
  - Direct function invocations via fmgr interface

## Notes and Other Information
- The function performs two levels of date validation: first checking if the parsed date components are valid for Julian day calculation, then verifying the final DateADT value is within PostgreSQL's supported range
- Uses POSTGRES_EPOCH_JDATE as the reference point for date calculations
- Error handling includes specific error codes (ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) and descriptive error messages
- The function ignores time components that might be parsed by do_to_timestamp, focusing only on date extraction
- Part of PostgreSQL's formatting subsystem located in src/backend/utils/adt/formatting.c

## Simplified Source

```c
Datum to_date(PG_FUNCTION_ARGS) {
    text *date_txt = PG_GETARG_TEXT_PP(0);
    text *fmt = PG_GETARG_TEXT_PP(1);
    Oid collid = PG_GET_COLLATION();
    DateADT result;
    struct pg_tm tm;
    struct fmt_tz ftz;
    fsec_t fsec;

    // Parse the date string using the format template
    do_to_timestamp(date_txt, fmt, collid, false,
                   &tm, &fsec, &ftz, NULL, NULL, NULL);

    // Validate date components for Julian day calculation
    if (!IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("date out of range: \"%s\"",
                              text_to_cstring(date_txt))));

    // Convert to PostgreSQL's internal date format
    result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    // Final validation of date range
    if (!IS_VALID_DATE(result))
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("date out of range: \"%s\"",
                              text_to_cstring(date_txt))));

    PG_RETURN_DATEADT(result);
}
```