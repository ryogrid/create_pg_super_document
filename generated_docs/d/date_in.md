# date_in

## Location
[src/backend/utils/adt/date.c:113-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L113-L183)

## Overview
Converts a date text string into PostgreSQL's internal date format, performing comprehensive parsing and validation of input date values.

## Definition

```c
struct pg_tm tt,
			   *tm = &tt;
```
## Detailed Description
This function serves as the input conversion routine for PostgreSQL's DATE data type. It parses a string representation of a date and converts it to the internal DateADT format. The function handles various date formats including regular dates, epoch references, and special values like 'infinity' and '-infinity'. It performs extensive validation including range checking and Julian date validity verification. The function supports soft error handling through the error context mechanism, allowing callers to handle parse errors gracefully.

## Parameters / Member Variables
- Input accessed via  macro:
  - : C-string containing the date to parse (accessed via )
  - : Error context node for soft error handling (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - [ParseDateTime](../P/ParseDateTime.md) (parses date string into field components)
  - [DecodeDateTime](../D/DecodeDateTime.md) (interprets parsed fields into date components)
  - [DateTimeParseError](../D/DateTimeParseError.md) (reports parsing errors)
  - [GetEpochTime](../G/GetEpochTime.md) (sets time to Unix epoch for DTK_EPOCH)
  - IS_VALID_JULIAN (validates Julian date range)
  - [date2j](date2j.md) (converts calendar date to Julian day number)
  - IS_VALID_DATE (validates final date range)
  - DATE_NOEND, DATE_NOBEGIN (special infinity values)
  - PG_RETURN_DATEADT (returns DateADT value)
- Data types and constants:
  - DateADT, fsec_t, pg_tm, DateTimeErrorExtra
  - MAXDATEFIELDS, MAXDATELEN, POSTGRES_EPOCH_JDATE
  - DTK_DATE, DTK_EPOCH, DTK_LATE, DTK_EARLY, DTERR_BAD_FORMAT
- Called from:
  - PostgreSQL type system as input function for DATE type (registered in system catalogs)

## Notes and Other Information
- This is the primary input function for PostgreSQL's DATE data type, registered in the system catalogs
- Supports special date values: 'epoch' (Unix epoch), 'infinity', '-infinity'
- Performs two-stage validation: Julian date validity and PostgreSQL date range validity
- Uses soft error handling mechanism - can return NULL on parse errors instead of throwing exceptions
- Converts dates to internal representation as days since PostgreSQL epoch (2000-01-01)
- Input string is processed through a work buffer to avoid modifying the original
- Fractional seconds are parsed but ignored for DATE type (fsec parameter unused)
- [Range](../R/Range.md) validation prevents integer overflow in Julian day calculations
- Part of PostgreSQL's date/time type system infrastructure

## Simplified Source

```c
Datum date_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    DateADT date;
    fsec_t fsec;
    struct pg_tm tt, *tm = &tt;
    int tzp, dtype, nf, dterr;
    char *field[MAXDATEFIELDS];
    int ftype[MAXDATEFIELDS];
    char workbuf[MAXDATELEN + 1];
    DateTimeErrorExtra extra;

    // Parse the input date string into fields
    dterr = ParseDateTime(str, workbuf, sizeof(workbuf), field, ftype, MAXDATEFIELDS, &nf);
    if (dterr == 0)
        dterr = DecodeDateTime(field, ftype, nf, &dtype, tm, &fsec, &tzp, &extra);

    // Handle parse errors
    if (dterr != 0) {
        DateTimeParseError(dterr, &extra, str, "date", escontext);
        PG_RETURN_NULL();
    }

    // Handle different date types
    switch (dtype) {
        case DTK_DATE:
            break;  // Normal date, continue processing
        case DTK_EPOCH:
            GetEpochTime(tm);  // Set to Unix epoch
            break;
        case DTK_LATE:
            DATE_NOEND(date);  // Infinity
            PG_RETURN_DATEADT(date);
        case DTK_EARLY:
            DATE_NOBEGIN(date);  // -Infinity
            PG_RETURN_DATEADT(date);
        default:
            DateTimeParseError(DTERR_BAD_FORMAT, &extra, str, "date", escontext);
            PG_RETURN_NULL();
    }

    // Validate Julian date range
    if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("date out of range: \"%s\"", str)));

    // Convert to internal date format (days since PostgreSQL epoch)
    date = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;

    // Final range validation
    if (!IS_VALID_DATE(date))
        ereturn(escontext, (Datum) 0,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("date out of range: \"%s\"", str)));

    PG_RETURN_DATEADT(date);
}
```