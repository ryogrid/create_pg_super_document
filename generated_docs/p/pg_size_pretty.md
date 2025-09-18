# pg_size_pretty

## Location
[src/backend/utils/adt/dbsize.c:569-610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L569-L610)

## Overview
This function formats a numeric byte size value into a human-readable string representation with appropriate size units (bytes, kB, MB, GB, TB, etc.).

## Definition


## Detailed Description
The pg_size_pretty function converts a 64-bit integer representing a size in bytes into a formatted string with appropriate size units for better readability. It uses a table of size units (size_pretty_units) to determine the most appropriate unit to display the size. The function intelligently handles:
- Selection of the most appropriate unit based on the magnitude of the size
- Rounding rules for different units
- Both positive and negative size values
- Unit scaling using bit shifting for efficient division

The algorithm iterates through available units and selects the first unit where the absolute size is below the unit's limit, or uses the largest available unit if the size exceeds all limits.

## Parameters / Member Variables
- Input parameter (via PG_GETARG_INT64(0)): A 64-bit signed integer representing size in bytes

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (extracts 64-bit integer argument)
  - size_pretty_unit (structure defining size units and their properties)
  - half_rounded (applies half-rounding to values when needed)
  - INT64_FORMAT (macro for formatting 64-bit integers)
  - cstring_to_text (converts C string to PostgreSQL text type)
  - PG_RETURN_TEXT_P (returns text result)
- Called from (representative examples):
  - SQL queries using pg_size_pretty() function
  - Database administration queries for displaying sizes

## Notes and Other Information
- Returns formatted strings like "1024 bytes", "1 kB", "1 MB", etc.
- Handles negative sizes by working with absolute values for unit selection
- Uses efficient bit-shifting division rather than standard division
- The rounding behavior varies by unit as defined in the size_pretty_units table
- Commonly used in database monitoring and administration tools for displaying storage sizes in readable format