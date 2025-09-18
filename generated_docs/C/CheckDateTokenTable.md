# CheckDateTokenTable

## Location
[src/backend/utils/adt/datetime.c:4779-4810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4779-L4810)

## Overview
A static validation function that checks the integrity and ordering of PostgreSQL's date/time token tables during system initialization.

## Definition
```c
static bool CheckDateTokenTable(const char *tablename, const datetkn *base, int nel)
```

## Detailed Description
This function performs critical validation of PostgreSQL's internal date/time token lookup tables to ensure they are properly formatted and sorted. It validates that all token strings fit within the maximum allowed length (TOKMAXLEN) and that the table entries are correctly sorted in lexicographic order. These checks are essential because the date/time parsing system relies on binary search algorithms that require sorted tables, and malformed tables would cause parsing failures or crashes.

## Parameters / Member Variables
- `tablename`: Name of the table being checked, used for error reporting to identify which specific token table has issues
- `base`: Pointer to the first element of the datetkn array being validated
- `nel`: Number of elements in the token table array

## Dependencies
- Functions called/Symbols referenced:
  - datetkn (PostgreSQL date/time token structure)
  - TOKMAXLEN (maximum token length constant)
  - strlen (standard C library function)
  - strcmp (standard C library function)
  - elog (PostgreSQL logging function)
- Called from (representative examples):
  - [CheckDateTokenTables](CheckDateTokenTables.md) (in src/backend/utils/adt/datetime.c)
  - [ConvertTimeZoneAbbrevs](ConvertTimeZoneAbbrevs.md) (in src/backend/utils/adt/datetime.c)

## Notes and Other Information
- Logs errors using LOG level, which allows administrators to detect table corruption issues during startup
- Returns false immediately upon finding the first error to prevent potential crashes from subsequent operations on malformed tables
- Part of PostgreSQL's defensive programming approach to catch configuration or build issues early
- The ordering validation is critical because date/time token lookup uses binary search for performance
- Validates both token length constraints and lexicographic ordering in a single pass
- Used during postmaster startup to ensure system integrity before accepting connections