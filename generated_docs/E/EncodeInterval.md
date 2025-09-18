# EncodeInterval

## Location
src/backend/utils/adt/datetime.c: 4585 - 4778

## Overview
Converts a PostgreSQL interval structure to its string representation, supporting multiple output formats including SQL Standard, ISO 8601, traditional PostgreSQL, and verbose PostgreSQL styles.

## Definition
```c
void EncodeInterval(struct pg_itm *itm, int style, char *str)
```

## Detailed Description
This function is the primary interface for converting PostgreSQL interval data structures to human-readable string representations. It supports four different output formats: SQL Standard format (with strict sign handling), ISO 8601 duration format (P1Y2M3DT4H5M6S style), traditional PostgreSQL format (compatible with versions < 8.4), and verbose PostgreSQL format (using full words like 'years', 'months', etc.). The function handles complex sign logic, zero value suppression, and format-specific requirements for each output style.

## Parameters / Member Variables
- `itm`: Pointer to a pg_itm structure containing the interval components (years, months, days, hours, minutes, seconds, microseconds)
- `style`: Integer constant specifying the desired output format (INTSTYLE_SQL_STANDARD, INTSTYLE_ISO_8601, INTSTYLE_POSTGRES, or INTSTYLE_POSTGRES_VERBOSE)
- `str`: Output buffer where the formatted interval string will be written

## Dependencies
- Functions called/Symbols referenced:
  - AddPostgresIntPart (for PostgreSQL format output)
  - AddVerboseIntPart (for verbose PostgreSQL format output)
  - AddISO8601IntPart (for ISO 8601 format output)
  - AppendSeconds (for formatting seconds with microseconds)
  - i64abs (for 64-bit absolute value calculations)
  - sprintf, strcpy, strcat (standard C string functions)
- Called from (representative examples):
  - interval_out (in src/backend/utils/adt/timestamp.c)
  - PGTYPESinterval_to_asc (in src/interfaces/ecpg/pgtypeslib/interval.c)

## Notes and Other Information
- Handles complex sign logic differently for each format style, with SQL Standard requiring single leading signs, ISO 8601 using individual field signs, and PostgreSQL styles using contextual sign handling
- Special handling for zero intervals to ensure meaningful output in all formats
- The function modifies local copies of interval components to handle sign normalization without affecting the input structure
- Supports backward compatibility with older PostgreSQL versions through the INTSTYLE_POSTGRES format
- Uses format-specific helper functions to maintain clean separation of formatting logic
- Part of PostgreSQL's core interval data type system, essential for interval output operations