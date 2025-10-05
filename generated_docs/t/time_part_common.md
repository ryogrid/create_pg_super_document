# time_part_common

## Location
[src/backend/utils/adt/date.c:2140-2242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2140-L2242)

## Overview
The  function extracts specified time components (hour, minute, second, etc.) from a TimeADT value, with support for both numeric and floating-point return formats.

## Definition

```c
struct pg_tm tt,
				   *tm = &tt;
```
## Detailed Description
This function is the core implementation for extracting time components from a time data type in PostgreSQL. It processes a text string specifying which time component to extract (e.g., 'hour', 'minute', 'second') and returns the corresponding value from a TimeADT input. The function supports multiple precision levels for seconds (microseconds, milliseconds, seconds) and handles special cases like epoch conversion. It can return results either as numeric values (when retnumeric is true) or as floating-point values (when retnumeric is false).

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - Text string specifying the time component to extract
  - Argument 1:  - The time value to extract the component from
- : Boolean flag determining return type (true for numeric, false for float8)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP, PG_GETARG_TIMEADT
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md) (text processing)
  - [DecodeUnits](../D/DecodeUnits.md), DecodeSpecial (time unit parsing)
  - [time2tm](time2tm.md) (time conversion)
  - [int64_div_fast_to_numeric](../i/int64_div_fast_to_numeric.md), int64_to_numeric (numeric conversion)
  - PG_RETURN_NUMERIC, PG_RETURN_FLOAT8 (return macros)
  - ereport (error reporting)
- Data types used:
  - TimeADT, text, fsec_t, pg_tm
  - Various DTK constants (DTK_HOUR, DTK_MINUTE, etc.)
- Called from (representative examples):
  - [time_part](time_part.md) (src/backend/utils/adt/date.c:2245)
  - [extract_time](../e/extract_time.md) (src/backend/utils/adt/date.c:2251)

## Notes and Other Information
- The function is static and serves as the common implementation for both time_part() and extract_time()
- Supports extraction of: microseconds, milliseconds, seconds, minutes, hours, and epoch
- Rejects unsupported time units (day, month, year, etc.) with appropriate error messages
- For sub-second precision, uses high-precision arithmetic to maintain accuracy
- The epoch extraction returns seconds since Unix epoch as a fractional value
- Input unit names are case-insensitive due to downcase_truncate_identifier processing
- Error handling includes both ERRCODE_FEATURE_NOT_SUPPORTED and ERRCODE_INVALID_PARAMETER_VALUE
- Located in src/backend/utils/adt/date.c:2140-2242

## Simplified Source

```c
static Datum
time_part_common(PG_FUNCTION_ARGS, bool retnumeric)
{
    // Extract function arguments
    text *units = PG_GETARG_TEXT_PP(0);
    TimeADT time = PG_GETARG_TIMEADT(1);

    // Parse the unit name (case-insensitive)
    char *lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
                                                 VARSIZE_ANY_EXHDR(units),
                                                 false);

    // Decode the unit type
    int type, val;
    type = DecodeUnits(0, lowunits, &val);
    if (type == UNKNOWN_FIELD)
        type = DecodeSpecial(0, lowunits, &val);

    if (type == UNITS) {
        // Convert time to tm structure for component extraction
        fsec_t fsec;
        struct pg_tm tt, *tm = &tt;
        time2tm(time, tm, &fsec);

        int64 intresult;
        switch (val) {
            case DTK_MICROSEC:
                intresult = tm->tm_sec * INT64CONST(1000000) + fsec;
                break;

            case DTK_MILLISEC:
                if (retnumeric)
                    PG_RETURN_NUMERIC(int64_div_fast_to_numeric(
                        tm->tm_sec * INT64CONST(1000000) + fsec, 3));
                else
                    PG_RETURN_FLOAT8(tm->tm_sec * 1000.0 + fsec / 1000.0);
                break;

            case DTK_SECOND:
                if (retnumeric)
                    PG_RETURN_NUMERIC(int64_div_fast_to_numeric(
                        tm->tm_sec * INT64CONST(1000000) + fsec, 6));
                else
                    PG_RETURN_FLOAT8(tm->tm_sec + fsec / 1000000.0);
                break;

            case DTK_MINUTE:
                intresult = tm->tm_min;
                break;

            case DTK_HOUR:
                intresult = tm->tm_hour;
                break;

            default:
                // Reject unsupported units for TIME type
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("unit \"%s\" not supported for type %s",
                                lowunits, format_type_be(TIMEOID))));
                intresult = 0;
        }

        // Return integer result
        if (retnumeric)
            PG_RETURN_NUMERIC(int64_to_numeric(intresult));
        else
            PG_RETURN_FLOAT8(intresult);
    }
    else if (type == RESERV && val == DTK_EPOCH) {
        // Special case: epoch conversion
        if (retnumeric)
            PG_RETURN_NUMERIC(int64_div_fast_to_numeric(time, 6));
        else
            PG_RETURN_FLOAT8(time / 1000000.0);
    }
    else {
        // Unrecognized unit
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("unit \"%s\" not recognized for type %s",
                        lowunits, format_type_be(TIMEOID))));
        PG_RETURN_FLOAT8(0);  // Never reached
    }
}
``` 