# PGTYPESdate_to_asc

## Location
[src/interfaces/ecpg/pgtypeslib/datetime.c:101-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/datetime.c#L101-L114)

## Overview
Converts a PostgreSQL date value to its string representation using PostgreSQL's standard date formatting conventions.

## Definition
```c
char *PGTYPESdate_to_asc(date dDate)
```

## Detailed Description
PGTYPESdate_to_asc converts a PostgreSQL internal date representation (days since 2000-01-01) back to a human-readable string format. The function first converts the date value from PostgreSQL's internal representation to a Julian date by adding the reference date (2000-01-01), then uses PostgreSQL's j2date function to convert the Julian date to year, month, and day components. These components are then formatted into a string using the EncodeDateOnly function with the specified DateStyle. The result is duplicated using pgtypes_strdup to return a newly allocated string that the caller owns and must free.

## Parameters / Member Variables
- `dDate`: The PostgreSQL date value to convert (days since 2000-01-01)

## Dependencies
- Functions called/Symbols referenced:
  - [j2date](../j/j2date.md) (Julian date to calendar date conversion)
  - [date2j](../d/date2j.md) (Calendar date to Julian date conversion - for reference point)
  - [EncodeDateOnly](../E/EncodeDateOnly.md) (PostgreSQL date encoding function)
  - [pgtypes_strdup](../p/pgtypes_strdup.md) (string duplication function)
  - MAXDATELEN (buffer size constant)
- Called from (representative examples):
  - [rdatestr](../r/rdatestr.md) (Informix compatibility function)
  - ecpg_store_input (ECPG data storage function)
  - [main](../m/main.md) (extensive use in test cases)

## Notes and Other Information
- Returns a dynamically allocated string that must be freed by the caller
- Uses DateStyle = 1 for formatting (PostgreSQL default date style)
- European date format is currently disabled (EuroDates = false)
- The function handles the conversion from PostgreSQL's internal date representation to standard calendar dates
- Buffer size is limited to MAXDATELEN + 1 characters for safety
- Part of the ECPG pgtypeslib interface for date-to-string conversions
- Located in src/interfaces/ecpg/pgtypeslib/datetime.c:101-114
- Widely used throughout ECPG test suites and compatibility libraries