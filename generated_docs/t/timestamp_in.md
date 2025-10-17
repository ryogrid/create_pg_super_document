# timestamp_in

## Location
[src/backend/utils/adt/timestamp.c:164-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L164-L231)

## Overview
A PostgreSQL input function that converts string representations of timestamps into internal timestamp format (without timezone), handling various input formats and special values.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
This function implements the input conversion for the TIMESTAMP data type (without timezone). It parses string representations of timestamps and converts them to PostgreSQL's internal timestamp format. The function handles a wide variety of input formats including ISO 8601, SQL standard formats, and PostgreSQL-specific special values like 'epoch', 'infinity', and '-infinity'.

The parsing process involves multiple stages: first tokenizing the input string using ParseDateTime, then interpreting the tokens with DecodeDateTime, and finally converting the parsed components into PostgreSQL's internal timestamp representation. The function also applies type modifier constraints (precision) and performs range validation.

## Parameters / Member Variables
-  (arg 0): Input string to be parsed as a timestamp
-  (arg 1): Type element OID (unused in current implementation)
-  (arg 2): Type modifier specifying precision constraints
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [ParseDateTime](../P/ParseDateTime.md): Initial string parsing and tokenization
  - [DecodeDateTime](../D/DecodeDateTime.md): Token interpretation and datetime component extraction
  - [DateTimeParseError](../D/DateTimeParseError.md): Error reporting for parsing failures
  - [tm2timestamp](tm2timestamp.md): Conversion from broken-down time to timestamp
  - [SetEpochTimestamp](../S/SetEpochTimestamp.md): Handling of 'epoch' special value
  - [AdjustTimestampForTypmod](../A/AdjustTimestampForTypmod.md): Applying precision constraints
  - TIMESTAMP_NOEND/TIMESTAMP_NOBEGIN: Handling infinity values
  - PG_RETURN_TIMESTAMP: Return value macro
- Called from: Used as input function for TIMESTAMP type (registered in pg_type catalog)

## Notes and Other Information
- Supports special values: 'epoch' (1970-01-01 00:00:00), 'infinity', '-infinity'
- Handles various datetime formats through PostgreSQL's flexible parsing engine
- Performs range checking and reports appropriate error codes for out-of-range values
- Uses soft error handling (escontext) to allow callers to handle errors gracefully
- The typmod parameter controls fractional seconds precision (0-6 digits)
- Input parsing is locale-aware and respects DateStyle settings
- Returns NULL on parsing errors when operating in soft error mode

## Simplified Source

```c
Datum timestamp_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    int32 typmod = PG_GETARG_INT32(2);
    Node *escontext = fcinfo->context;
    Timestamp result;
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    int tz, dtype, nf, dterr;
    char *field[MAXDATEFIELDS];
    int ftype[MAXDATEFIELDS];
    char workbuf[MAXDATELEN + MAXDATEFIELDS];
    DateTimeErrorExtra extra;

    // Parse the input string into tokens
    dterr = ParseDateTime(str, workbuf, sizeof(workbuf),
                         field, ftype, MAXDATEFIELDS, &nf);
    if (dterr == 0) {
        // Decode tokens into datetime components
        dterr = DecodeDateTime(field, ftype, nf,
                              &dtype, tm, &fsec, &tz, &extra);
    }

    // Handle parsing errors
    if (dterr != 0) {
        DateTimeParseError(dterr, &extra, str, "timestamp", escontext);
        PG_RETURN_NULL();
    }

    // Convert parsed components to timestamp based on type
    switch (dtype) {
        case DTK_DATE:
            if (tm2timestamp(tm, fsec, NULL, &result) != 0) {
                ereturn(escontext, (Datum) 0,
                       (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        errmsg("timestamp out of range: \"%s\"", str)));
            }
            break;
        case DTK_EPOCH:
            result = SetEpochTimestamp();
            break;
        case DTK_LATE:
            TIMESTAMP_NOEND(result);
            break;
        case DTK_EARLY:
            TIMESTAMP_NOBEGIN(result);
            break;
        default:
            elog(ERROR, "unexpected dtype %d while parsing timestamp \"%s\"",
                 dtype, str);
            TIMESTAMP_NOEND(result);
    }

    // Apply precision constraints
    AdjustTimestampForTypmod(&result, typmod, escontext);

    PG_RETURN_TIMESTAMP(result);
}
```