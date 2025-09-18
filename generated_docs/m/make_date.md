# make_date

## Location
[src/backend/utils/adt/date.c:245-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L245-L293)

## Overview
Constructs a PostgreSQL date value from individual year, month, and day components with comprehensive validation and error checking.

## Definition
```c
Datum make_date(PG_FUNCTION_ARGS)
```

## Detailed Description
The `make_date` function serves as a date constructor that creates a PostgreSQL DateADT value from separate integer components representing year, month, and day. This function implements comprehensive validation at multiple levels: it validates individual field values, checks for valid Julian calendar dates, and ensures the resulting date falls within PostgreSQL's supported date range. The function handles negative years by treating them as BC (Before Christ) dates, converting them to positive values for internal processing. The final result is computed using Julian day calculations relative to the PostgreSQL epoch.

## Parameters / Member Variables
- `tm.tm_year`: Integer year component (negative values treated as BC)
- `tm.tm_mon`: Integer month component (1-12)
- `tm.tm_mday`: Integer day component (1-31 depending on month)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32: Macro to extract 32-bit integer arguments
  - ValidateDate: Validates date field values and relationships
  - IS_VALID_JULIAN: Macro to check if date components form a valid Julian date
  - [date2j](../d/date2j.md): Converts year/month/day to Julian day number
  - IS_VALID_DATE: Macro to validate DateADT values are within PostgreSQL range
  - ereport: PostgreSQL error reporting function
  - PG_RETURN_DATEADT: Macro to return DateADT values
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- This function corresponds to the SQL MAKE_DATE() function
- Negative year values are automatically converted to BC dates
- Multiple validation stages prevent invalid dates and overflow conditions
- Uses Julian day arithmetic for accurate date calculations
- Error messages include the problematic date values for debugging
- Returns ERRCODE_DATETIME_FIELD_OVERFLOW for invalid field values
- Returns ERRCODE_DATETIME_VALUE_OUT_OF_RANGE for dates outside PostgreSQL's supported range
- The function follows PostgreSQL's standard function calling conventions