# interval_trunc

## Location
[src/backend/utils/adt/timestamp.c:5017-5115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5017-L5115)

## Overview
Truncates an interval value to specified units by zeroing out all more precise components while preserving larger units.

## Definition

```c
struct pg_itm tt,
			   *tm = &tt;
```
## Detailed Description
This function truncates an interval to the specified time unit precision. It converts the interval to an internal time structure (pg_itm), then systematically zeros out all time components that are more precise than the specified unit. For example, truncating to 'hour' will zero out minutes, seconds, and microseconds while preserving years, months, days, and hours. The function uses a cascading switch statement with fall-through behavior to implement the truncation logic efficiently. Special handling is provided for units like quarter (rounds to nearest 3-month boundary) and millisecond precision.

## Parameters / Member Variables
-  (text): The time unit to truncate to (e.g., 'millennium', 'century', 'decade', 'year', 'quarter', 'month', 'day', 'hour', 'minute', 'second', 'millisec', 'microsec')
-  (Interval*): The interval value to be truncated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (for extracting text argument)
  - PG_GETARG_INTERVAL_P (for extracting interval argument) 
  - [palloc](../p/palloc.md) (for memory allocation)
  - INTERVAL_NOT_FINITE (macro for checking infinite intervals)
  - memcpy (for copying infinite interval values)
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md) (for normalizing unit names)
  - [DecodeUnits](../D/DecodeUnits.md) (for parsing time unit strings)
  - [interval2itm](interval2itm.md) (for converting interval to internal time structure)
  - [itm2interval](itm2interval.md) (for converting back to interval)
  - ereport/errcode/errmsg (for error reporting)
  - [format_type_be](../f/format_type_be.md) (for formatting type names in errors)
  - PG_RETURN_INTERVAL_P (for returning result)
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- Handles infinite interval values by returning them unchanged
- Uses cascading switch statement with fall-through to implement efficient truncation
- Special handling for quarters (truncates to 3-month boundaries)
- Week truncation is explicitly not supported due to the complexity of fractional weeks in months
- Millisecond truncation preserves microsecond precision in thousands
- Includes comprehensive error handling for unsupported or unrecognized units
- The function carefully handles C division behavior for negative remainders when truncating larger units
- Located in src/backend/utils/adt/timestamp.c:5017-5115

## Simplified Source

```c
Datum interval_trunc(PG_FUNCTION_ARGS) {
    text *units = PG_GETARG_TEXT_PP(0);
    Interval *interval = PG_GETARG_INTERVAL_P(1);
    Interval *result = (Interval *) palloc(sizeof(Interval));
    int type, val;
    char *lowunits;
    struct pg_itm tm;

    // Return infinite intervals as-is
    if (INTERVAL_NOT_FINITE(interval)) {
        memcpy(result, interval, sizeof(Interval));
        PG_RETURN_INTERVAL_P(result);
    }

    // Parse unit string (case-insensitive)
    lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
                                           VARSIZE_ANY_EXHDR(units), false);
    type = DecodeUnits(0, lowunits, &val);

    if (type == UNITS) {
        // Convert interval to internal time structure
        interval2itm(*interval, &tm);

        // Truncate based on specified unit (cascading fall-through)
        switch (val) {
            case DTK_MILLENNIUM:
                // Note: C division may have negative remainder
                tm.tm_year = (tm.tm_year / 1000) * 1000;
                /* FALL THRU */
            case DTK_CENTURY:
                tm.tm_year = (tm.tm_year / 100) * 100;
                /* FALL THRU */
            case DTK_DECADE:
                tm.tm_year = (tm.tm_year / 10) * 10;
                /* FALL THRU */
            case DTK_YEAR:
                tm.tm_mon = 0;
                /* FALL THRU */
            case DTK_QUARTER:
                tm.tm_mon = 3 * (tm.tm_mon / 3);
                /* FALL THRU */
            case DTK_MONTH:
                tm.tm_mday = 0;
                /* FALL THRU */
            case DTK_DAY:
                tm.tm_hour = 0;
                /* FALL THRU */
            case DTK_HOUR:
                tm.tm_min = 0;
                /* FALL THRU */
            case DTK_MINUTE:
                tm.tm_sec = 0;
                /* FALL THRU */
            case DTK_SECOND:
                tm.tm_usec = 0;
                break;
            case DTK_MILLISEC:
                tm.tm_usec = (tm.tm_usec / 1000) * 1000;
                break;
            case DTK_MICROSEC:
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("unit \"%s\" not supported for type %s",
                                      lowunits, format_type_be(INTERVALOID)),
                               (val == DTK_WEEK) ? errdetail("Months usually have fractional weeks.") : 0));
        }

        // Convert back to interval
        if (itm2interval(&tm, result) != 0)
            ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                           errmsg("interval out of range")));
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("unit \"%s\" not recognized for type %s",
                              lowunits, format_type_be(INTERVALOID))));
    }

    PG_RETURN_INTERVAL_P(result);
}
```