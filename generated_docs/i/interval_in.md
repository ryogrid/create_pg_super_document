# interval_in

## Location
[src/backend/utils/adt/timestamp.c:900-981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L900-L981)

## Overview
Converts a string representation to PostgreSQL's internal Interval data type, supporting multiple input formats including standard SQL interval syntax and ISO8601 format.

## Definition
```c
Datum interval_in(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_in` function is the input conversion function for PostgreSQL's interval data type. It parses string representations of time intervals and converts them to PostgreSQL's internal Interval structure. The function supports multiple input formats through a sophisticated parsing pipeline that first attempts standard PostgreSQL datetime parsing, then falls back to ISO8601 interval format if the initial parsing fails. It handles special interval values like infinity (early/late) and applies type modifiers for precision control.

## Parameters / Member Variables
- `str` (PG_GETARG_CSTRING(0)): Input string representation of the interval
- `typelem` (unused): Type element OID (not currently used)
- `typmod` (PG_GETARG_INT32(2)): Type modifier specifying interval precision and range restrictions
- `escontext`: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [ParseDateTime](../P/ParseDateTime.md) (initial parsing attempt)
  - [DecodeInterval](../D/DecodeInterval.md) (decode parsed fields into interval)
  - [DecodeISO8601Interval](../D/DecodeISO8601Interval.md) (fallback ISO8601 parsing)
  - [itmin2interval](itmin2interval.md) (convert internal time structure to Interval)
  - [AdjustIntervalForTypmod](../A/AdjustIntervalForTypmod.md) (apply type modifier constraints)
  - [DateTimeParseError](../D/DateTimeParseError.md) (error reporting)
  - INTERVAL_RANGE, INTERVAL_FULL_RANGE (typmod handling)
  - INTERVAL_NOEND, INTERVAL_NOBEGIN (special infinity values)
- Called from (representative examples):
  - [check_timezone](../c/check_timezone.md) (src/backend/commands/variable.c:299)
  - [flatten_set_variable_args](../f/flatten_set_variable_args.md) (src/backend/utils/misc/guc_funcs.c:276)

## Notes and Other Information
- Supports multiple input formats: standard PostgreSQL syntax and ISO8601 intervals
- Handles special values for infinite intervals (DTK_LATE, DTK_EARLY)
- Uses a two-stage parsing approach with fallback for better format compatibility
- Applies precision and range constraints through typmod processing
- Uses soft error handling through escontext for better error reporting
- The function initializes a pg_itm_in structure to zero before parsing to ensure clean state

## Simplified Source

```c
Datum
interval_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    int32 typmod = PG_GETARG_INT32(2);
    Node *escontext = fcinfo->context;
    Interval *result;
    struct pg_itm_in tt, *itm_in = &tt;
    int dtype, nf, range, dterr;
    char *field[MAXDATEFIELDS];
    int ftype[MAXDATEFIELDS];
    char workbuf[256];
    DateTimeErrorExtra extra;

    // Initialize time structure
    itm_in->tm_year = 0;
    itm_in->tm_mon = 0;
    itm_in->tm_mday = 0;
    itm_in->tm_usec = 0;

    // Extract range from typmod
    range = (typmod >= 0) ? INTERVAL_RANGE(typmod) : INTERVAL_FULL_RANGE;

    // Parse string using standard PostgreSQL format
    dterr = ParseDateTime(str, workbuf, sizeof(workbuf), field, ftype, MAXDATEFIELDS, &nf);
    if (dterr == 0)
        dterr = DecodeInterval(field, ftype, nf, range, &dtype, itm_in);

    // Try ISO8601 format if standard parsing failed
    if (dterr == DTERR_BAD_FORMAT)
        dterr = DecodeISO8601Interval(str, &dtype, itm_in);

    // Handle parsing errors
    if (dterr != 0) {
        if (dterr == DTERR_FIELD_OVERFLOW)
            dterr = DTERR_INTERVAL_OVERFLOW;
        DateTimeParseError(dterr, &extra, str, "interval", escontext);
        PG_RETURN_NULL();
    }

    result = (Interval *) palloc(sizeof(Interval));

    // Convert based on parsed data type
    switch (dtype) {
        case DTK_DELTA:
            if (itmin2interval(itm_in, result) != 0)
                ereturn(escontext, (Datum) 0, "interval out of range");
            break;

        case DTK_LATE:
            INTERVAL_NOEND(result);
            break;

        case DTK_EARLY:
            INTERVAL_NOBEGIN(result);
            break;

        default:
            elog(ERROR, "unexpected dtype %d while parsing interval \"%s\"", dtype, str);
    }

    // Apply type modifier constraints
    AdjustIntervalForTypmod(result, typmod, escontext);

    PG_RETURN_INTERVAL_P(result);
}
```