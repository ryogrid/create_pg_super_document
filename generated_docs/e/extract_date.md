# extract_date

## Location
[src/backend/utils/adt/date.c:1066-1245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1066-L1245)

## Overview
Extracts specified fields (like year, month, day, etc.) from a PostgreSQL date value and returns the result as a numeric value.

## Definition
```c
Datum extract_date(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the EXTRACT functionality for PostgreSQL date data types. It takes a text unit specification (such as 'year', 'month', 'day', 'quarter', etc.) and a date value, then extracts the specified component and returns it as a numeric result.

The function handles a wide variety of date components including:
- Basic components: day, month, year
- Derived components: quarter, week, decade, century, millennium
- Special components: Julian day, ISO year, day of week, day of year, epoch
- Infinite date handling: properly handles positive and negative infinity dates

For infinite dates, oscillating units (day, month, quarter, week, dow, isodow, doy) return NULL, while monotonically-increasing units (year, decade, century, millennium, julian, isoyear, epoch) return appropriate infinity values.

The function uses PostgreSQL's internal date conversion functions and follows SQL standard semantics for date component extraction.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: Text specification of the unit to extract (units)
- `PG_GETARG_DATEADT(1)`: The date value to extract from (date)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TEXT_PP`, `PG_GETARG_DATEADT` - Argument extraction macros
  - [downcase_truncate_identifier](../d/downcase_truncate_identifier.md) - [String](../S/String.md) processing for unit names
  - [DecodeUnits](../D/DecodeUnits.md), `DecodeSpecial` - Unit parsing functions
  - [j2date](../j/j2date.md) - Julian to Gregorian date conversion
  - [date2isoweek](../d/date2isoweek.md), `date2isoyear` - ISO week/year calculations
  - `[j2day](../j/j2day.md)` - Julian to day-of-week conversion
  - [date2j](../d/date2j.md) - Gregorian to Julian date conversion
  - [int64_to_numeric](../i/int64_to_numeric.md) - [Numeric](../N/Numeric.md) result conversion
  - `DATE_NOT_FINITE`, `DATE_IS_NOBEGIN` - Infinite date checks
  - Various `DTK_*` constants for date/time field types
  - `POSTGRES_EPOCH_JDATE`, `UNIX_EPOCH_JDATE`, `SECS_PER_DAY` - Epoch constants
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's EXTRACT function infrastructure for date types
- Handles special cases for infinite dates with appropriate semantics
- Supports both standard SQL and PostgreSQL-specific date components
- Located in src/backend/utils/adt/date.c:1066-1245
- Returns results as PostgreSQL numeric type to handle large values
- Follows same logic patterns as timestamp extraction functions
- BC (Before Christ) years are handled with special adjustment logic (no year 0)

## Simplified Source

```c
Datum
extract_date(PG_FUNCTION_ARGS)
{
    // Extract arguments
    text *units = PG_GETARG_TEXT_PP(0);
    DateADT date = PG_GETARG_DATEADT(1);
    int64 intresult;
    int type, val;

    // Parse the unit string
    char *lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
                                                  VARSIZE_ANY_EXHDR(units),
                                                  false);

    // Decode the requested unit type
    type = DecodeUnits(0, lowunits, &val);
    if (type == UNKNOWN_FIELD)
        type = DecodeSpecial(0, lowunits, &val);

    // Handle infinite dates
    if (DATE_NOT_FINITE(date) && (type == UNITS || type == RESERV)) {
        // Return NULL for oscillating units, infinity for monotonic units
        if (val == DTK_DAY || val == DTK_MONTH || val == DTK_QUARTER ||
            val == DTK_WEEK || val == DTK_DOW || val == DTK_ISODOW || val == DTK_DOY) {
            PG_RETURN_NULL();
        }
        // Return appropriate infinity value for monotonic units
        if (DATE_IS_NOBEGIN(date))
            PG_RETURN_NUMERIC(DatumGetNumeric(DirectFunctionCall3(numeric_in,
                                                                  CStringGetDatum("-Infinity"),
                                                                  ObjectIdGetDatum(InvalidOid),
                                                                  Int32GetDatum(-1))));
        else
            PG_RETURN_NUMERIC(DatumGetNumeric(DirectFunctionCall3(numeric_in,
                                                                  CStringGetDatum("Infinity"),
                                                                  ObjectIdGetDatum(InvalidOid),
                                                                  Int32GetDatum(-1))));
    }

    // Handle regular date extraction
    if (type == UNITS) {
        // Convert to year/month/day components
        int year, mon, mday;
        j2date(date + POSTGRES_EPOCH_JDATE, &year, &mon, &mday);

        // Extract the requested component
        switch (val) {
            case DTK_DAY:
                intresult = mday;
                break;
            case DTK_MONTH:
                intresult = mon;
                break;
            case DTK_QUARTER:
                intresult = (mon - 1) / 3 + 1;
                break;
            case DTK_WEEK:
                intresult = date2isoweek(year, mon, mday);
                break;
            case DTK_YEAR:
                intresult = (year > 0) ? year : year - 1;
                break;
            case DTK_DECADE:
                intresult = (year >= 0) ? year / 10 : -((8 - (year - 1)) / 10);
                break;
            case DTK_CENTURY:
                intresult = (year > 0) ? (year + 99) / 100 : -((99 - (year - 1)) / 100);
                break;
            case DTK_MILLENNIUM:
                intresult = (year > 0) ? (year + 999) / 1000 : -((999 - (year - 1)) / 1000);
                break;
            case DTK_JULIAN:
                intresult = date + POSTGRES_EPOCH_JDATE;
                break;
            case DTK_ISOYEAR:
                intresult = date2isoyear(year, mon, mday);
                if (intresult <= 0) intresult -= 1;
                break;
            case DTK_DOW:
            case DTK_ISODOW:
                intresult = j2day(date + POSTGRES_EPOCH_JDATE);
                if (val == DTK_ISODOW && intresult == 0) intresult = 7;
                break;
            case DTK_DOY:
                intresult = date2j(year, mon, mday) - date2j(year, 1, 1) + 1;
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("unit \"%s\" not supported for type %s",
                                      lowunits, format_type_be(DATEOID))));
                intresult = 0;
        }
    } else if (type == RESERV) {
        // Handle special reserved words like 'epoch'
        switch (val) {
            case DTK_EPOCH:
                intresult = ((int64) date + POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY;
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("unit \"%s\" not supported for type %s",
                                      lowunits, format_type_be(DATEOID))));
                intresult = 0;
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("unit \"%s\" not recognized for type %s",
                              lowunits, format_type_be(DATEOID))));
        intresult = 0;
    }

    PG_RETURN_NUMERIC(int64_to_numeric(intresult));
}
```