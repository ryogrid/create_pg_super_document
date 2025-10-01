# timestamptz_out

## Location
[src/backend/utils/adt/timestamp.c:785-812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L785-L812)

## Overview
Converts a timestamptz (timestamp with time zone) value to its external string representation for output purposes.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function is responsible for converting PostgreSQL's internal timestamptz representation to a human-readable string format. This function handles both finite and infinite timestamp values, properly formatting them according to the current DateStyle setting while including timezone information. It serves as the output function for the timestamptz data type, called whenever a timestamptz value needs to be displayed or exported.

The function follows PostgreSQL's standard input/output function convention, taking arguments through the  macro and returning a  containing a C string representation of the timestamp.

## Parameters / Member Variables
- Input: A timestamptz value obtained through 
- Output: A  containing a C string representation of the timestamp

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract timestamptz argument
  - : Macro to check for infinite timestamps
  - : Handles encoding of infinite timestamp values
  - : Converts timestamp to broken-down time structure
  - : Formats the timestamp according to DateStyle settings
  - : Creates a palloc'd copy of the formatted string
  - : Macro to return the C string result
- Called from (representative examples):
  - : Used during JSON value extraction

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:785-812
- Handles special timestamp values (infinity, -infinity) through 
- Uses timezone information from  to include proper timezone representation
- Returns a palloc'd string that must be freed by the caller
- Throws an error if the timestamp value is out of range
- The output format depends on the current  setting

## Simplified Source

```c
Datum timestamptz_out(PG_FUNCTION_ARGS) {
    TimestampTz dt = PG_GETARG_TIMESTAMPTZ(0);
    char *result;
    char buf[MAXDATELEN + 1];

    // Handle special timestamp values (infinity, -infinity)
    if (TIMESTAMP_NOT_FINITE(dt)) {
        EncodeSpecialTimestamp(dt, buf);
    }
    // Convert finite timestamp to formatted string
    else if (timestamp2tm(dt, &tz, tm, &fsec, &tzn, NULL) == 0) {
        EncodeDateTime(tm, fsec, true, tz, tzn, DateStyle, buf);
    }
    // Handle conversion errors
    else {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("timestamp out of range")));
    }

    // Return palloc'd copy of formatted string
    result = pstrdup(buf);
    PG_RETURN_CSTRING(result);
}
```