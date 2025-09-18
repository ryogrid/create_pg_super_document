# date_out

## Location
[src/backend/utils/adt/date.c:184-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L184-L208)

## Overview
Converts PostgreSQL's internal date format to a human-readable text string representation, handling both finite dates and special values like infinity.

## Definition


## Detailed Description
This function serves as the output conversion routine for PostgreSQL's DATE data type. It takes an internal DateADT value and converts it to a string representation according to the current DateStyle setting. The function handles both regular finite dates and special infinite values ('infinity' and '-infinity'). For finite dates, it converts from the internal Julian day representation back to calendar components and then formats them according to the configured date style. For infinite dates, it uses specialized encoding to produce the appropriate string representation.

## Parameters / Member Variables
- Input accessed via  macro:
  - Fri Sep 12 02:20:07 JST 2025: DateADT value representing the internal date (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - DATE_NOT_FINITE (checks if date is infinite)
  - [EncodeSpecialDate](../E/EncodeSpecialDate.md) (formats infinite date values)
  - [j2date](../j/j2date.md) (converts Julian day to calendar date components)
  - [EncodeDateOnly](../E/EncodeDateOnly.md) (formats finite date according to DateStyle)
  - [pstrdup](../p/pstrdup.md) (duplicates formatted string)
  - PG_RETURN_CSTRING (returns C-string result)
- Data types and constants:
  - DateADT, pg_tm
  - MAXDATELEN, POSTGRES_EPOCH_JDATE, DateStyle
- Called from (representative examples):
  - [ExecGetJsonValueItemString](../E/ExecGetJsonValueItemString.md) (src/backend/executor/execExprInterp.c:4513)
  - PostgreSQL type system as output function for DATE type (registered in system catalogs)

## Notes and Other Information
- This is the primary output function for PostgreSQL's DATE data type, registered in the system catalogs
- Handles special infinite date values: 'infinity' and '-infinity' 
- Respects the current DateStyle configuration (ISO, SQL, German, or Postgres format)
- Converts from internal representation (days since PostgreSQL epoch 2000-01-01) back to calendar date
- Uses Julian day calculations for accurate date arithmetic across different calendar systems
- The returned string is allocated in the current memory context
- Output buffer size is limited by MAXDATELEN to prevent overflow
- Part of PostgreSQL's date/time type system infrastructure for displaying dates to users
- Used internally by JSON functions when converting dates to string representations