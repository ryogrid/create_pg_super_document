# time_in

## Location
[src/backend/utils/adt/date.c:1374-1415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1374-L1415)

## Overview
Parses a string representation of a time value and converts it to PostgreSQL's internal TimeADT format with optional precision adjustment.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
The  function is a PostgreSQL built-in input function that parses a string representation of a time value and converts it to the internal TimeADT data type. This function is part of PostgreSQL's time type input/output system and handles various time string formats. The function performs comprehensive parsing using the PostgreSQL datetime parsing infrastructure, including error handling and precision adjustment based on the type modifier.

The parsing process involves multiple stages: first parsing the input string into fields using , then decoding those fields specifically as time-only values using , and finally converting the parsed components into the internal time representation using . The function also applies any specified precision constraints using .

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure
  - Argument 0:  - Input string containing time representation to parse
  - Argument 1:  - Type element OID (currently unused, marked with NOT_USED)
  - Argument 2:  - Type modifier for precision constraints
  - : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to extract string argument
  -  - Macro to extract OID argument (unused)
  -  - Macro to extract int32 type modifier
  -  - Core datetime string parsing function
  -  - Function to decode parsed fields as time values
  -  - Error reporting for parsing failures
  -  - Function to convert time structure to TimeADT
  -  - Function to apply precision constraints
  -  - Macro to return TimeADT result
  -  - Macro to return NULL on error
- Types used:
  -  - PostgreSQL internal time type
  -  - Fractional seconds type
  -  - Time structure
  -  - Error context node type
  -  - Extended error information structure
  -  - PostgreSQL generic return type
- Constants used:
  -  - Maximum length for date string processing
  -  - Maximum number of parsed date/time fields
- Called from: 
  - No direct references found (likely called through PostgreSQL's type system)

## Notes and Other Information
- This function is part of PostgreSQL's time data type implementation
- Handles various time string formats through the flexible ParseDateTime infrastructure
- Supports soft error handling through the escontext mechanism for improved error reporting
- Applies type modifier constraints to adjust precision (e.g., TIME(3) vs TIME(6))
- The unused typelem parameter is marked with NOT_USED, indicating it's not currently utilized
- Located in 
- Forms part of the Time ADT implementation section of the date/time utilities
- Error handling includes detailed error messages through DateTimeParseError
- Used internally by PostgreSQL when converting string literals to time values
- Supports timezone parsing but ignores timezone information for plain time values

## Simplified Source

```c
Datum
time_in(PG_FUNCTION_ARGS)
{
    // Extract function arguments
    char *str = PG_GETARG_CSTRING(0);
    int32 typmod = PG_GETARG_INT32(2);
    Node *escontext = fcinfo->context;

    TimeADT result;
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    int tz, nf, dterr;
    char workbuf[MAXDATELEN + 1];
    char *field[MAXDATEFIELDS];
    int dtype, ftype[MAXDATEFIELDS];
    DateTimeErrorExtra extra;

    // Parse the input string into date/time fields
    dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
                         field, ftype, MAXDATEFIELDS, &nf);

    // Decode the fields as time-only values
    if (dterr == 0)
        dterr = DecodeTimeOnly(field, ftype, nf,
                              &dtype, tm, &fsec, &tz, &extra);

    // Handle parsing errors
    if (dterr != 0) {
        DateTimeParseError(dterr, &extra, str, "time", escontext);
        PG_RETURN_NULL();
    }

    // Convert parsed time to internal format and apply type modifier
    tm2time(tm, fsec, &result);
    AdjustTimeForTypmod(&result, typmod);

    PG_RETURN_TIMEADT(result);
}
```