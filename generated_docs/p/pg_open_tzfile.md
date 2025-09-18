# pg_open_tzfile

## Location
[src/bin/initdb/findtimezone.c:65-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/findtimezone.c#L65-L90)

## Overview
Opens a timezone data file given a timezone name and returns the file descriptor, providing a simplified interface for accessing timezone files in the initdb context.

## Definition


## Detailed Description
The pg_open_tzfile function is a utility that opens timezone data files by constructing the full path from the timezone directory and the provided timezone name. This is a simplified version of the backend function with the same name, designed specifically for the initdb process.

Key characteristics:
- Assumes the input timezone name already has the correct case, eliminating the need for case-folding operations
- Constructs the full path by combining the timezone directory (from pg_TZDIR()) with the provided name
- Performs basic path length validation to prevent buffer overflows
- Opens the file in read-only mode with binary flag set

The function optionally returns the canonical name of the timezone through the canonname parameter, though in this simplified version it's just a copy of the input name.

## Parameters / Member Variables
- : The timezone name to open (e.g., "America/New_York")
- : Optional output buffer to store the canonical spelling of the timezone name (must be > TZ_STRLEN_MAX bytes). Can be NULL if canonical name is not needed.

## Dependencies
- Functions called/Symbols referenced:
  - strlcpy (for safe string copying)
  - [pg_TZDIR](pg_TZDIR.md) (to get timezone directory path)
  - open (system call to open the file)
  - TZ_STRLEN_MAX (constant defining maximum timezone name length)
  - PG_BINARY (flag for binary file access)
- Called from (representative examples):
  - tzloadbody (in src/timezone/localtime.c)
  - [pg_tz](pg_tz.md) (referenced in src/timezone/pgtz.h)

## Notes and Other Information
- Returns the file descriptor on success, -1 on failure
- This is a simplified version compared to the backend's pg_open_tzfile, as it doesn't perform case-folding
- The function assumes timezone names come from trusted sources (filesystem or TZ environment variable)
- [Path](../P/Path.md) construction is done using basic string operations with overflow protection
- The function is primarily used during database initialization when setting up timezone handling
- The canonname parameter provides compatibility with the backend version but serves a reduced purpose in this context