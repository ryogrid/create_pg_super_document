# date_in

## Location
src/backend/utils/adt/date.c: 113 - 183

## Overview
Converts a date text string into PostgreSQL's internal date format, performing comprehensive parsing and validation of input date values.

## Definition


## Detailed Description
This function serves as the input conversion routine for PostgreSQL's DATE data type. It parses a string representation of a date and converts it to the internal DateADT format. The function handles various date formats including regular dates, epoch references, and special values like 'infinity' and '-infinity'. It performs extensive validation including range checking and Julian date validity verification. The function supports soft error handling through the error context mechanism, allowing callers to handle parse errors gracefully.

## Parameters / Member Variables
- Input accessed via  macro:
  - : C-string containing the date to parse (accessed via )
  - : Error context node for soft error handling (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - ParseDateTime (parses date string into field components)
  - DecodeDateTime (interprets parsed fields into date components)
  - DateTimeParseError (reports parsing errors)
  - GetEpochTime (sets time to Unix epoch for DTK_EPOCH)
  - IS_VALID_JULIAN (validates Julian date range)
  - date2j (converts calendar date to Julian day number)
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
- Range validation prevents integer overflow in Julian day calculations
- Part of PostgreSQL's date/time type system infrastructure