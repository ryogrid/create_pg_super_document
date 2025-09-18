# pg_tzenumerate_start

## Location
[src/timezone/pgtz.c:397-413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/pgtz.c#L397-L413)

## Overview
Initializes timezone enumeration by creating and setting up a pg_tzenum structure to traverse the timezone directory hierarchy starting from the PostgreSQL timezone data directory.

## Definition
```c
pg_tzenum *pg_tzenumerate_start(void)
```

## Detailed Description
This function begins the process of enumerating all available timezone names in the PostgreSQL timezone database. It creates and initializes a pg_tzenum structure that maintains the state needed for recursive directory traversal through the timezone data directory tree.

The function allocates memory for the enumeration state structure, determines the starting directory using pg_TZDIR(), and opens the root timezone directory for reading. It sets up the initial traversal state with depth 0 and records the base directory path length for later path construction during enumeration.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pg_tzenum (structure type for enumeration state)
  - [pg_TZDIR](pg_TZDIR.md) (function to get timezone data directory path)
  - AllocateDir (PostgreSQL directory allocation function)
  - [palloc0](palloc0.md) (PostgreSQL memory allocation with zero initialization)
  - [pstrdup](pstrdup.md) (PostgreSQL string duplication)
  - ereport/ERROR (error reporting for directory access failures)
- Called from (representative examples):
  - [pg_timezone_names](pg_timezone_names.md) (in datetime.c to enumerate all timezone names)

## Notes and Other Information
- Returns a newly allocated pg_tzenum structure that must be freed with pg_tzenumerate_end()
- The pg_tzenum structure contains arrays for directory descriptors and names to support nested directory traversal
- Will raise an ERROR if the timezone directory cannot be opened
- The baselen field stores the length of the base timezone directory path plus 1
- Designed to work with pg_tzenumerate_next() for iterating through timezones
- Part of a trilogy of functions: start, next, and end for timezone enumeration
- Uses PostgreSQL's memory management (palloc0) rather than standard malloc