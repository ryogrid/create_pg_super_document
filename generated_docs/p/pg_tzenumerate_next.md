# pg_tzenumerate_next

## Location
[src/timezone/pgtz.c:426-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/pgtz.c#L426-L497)

## Overview
Iterates through the timezone directory hierarchy to find and return the next valid timezone, performing recursive directory traversal and timezone validation.

## Definition
```c
pg_tz *pg_tzenumerate_next(pg_tzenum *dir)
```

## Detailed Description
This function implements the core logic for timezone enumeration by traversing the timezone directory tree and returning each valid timezone found. It performs depth-first recursive directory traversal, automatically descending into subdirectories and backing out when directories are exhausted.

For each file encountered, the function determines if it's a directory (which triggers deeper traversal) or a timezone file (which gets loaded and validated). The function loads timezone files using tzload() directly rather than pg_tzset() to avoid filling the timezone cache, and validates each timezone using pg_tz_acceptable() to filter out unsupported timezones like leap-second zones.

The function maintains traversal state in the pg_tzenum structure, tracking the current directory depth and keeping directory handles open for efficient iteration. When a valid timezone is found, it returns a pointer to the timezone structure with the canonical name populated.

## Parameters / Member Variables
- `dir`: Pointer to the pg_tzenum structure containing enumeration state and directory traversal information

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tzenum](pg_tzenum.md) (enumeration state structure)
  - [dirent](../d/dirent.md) (directory entry structure)
  - [ReadDir](../R/ReadDir.md) (read directory entries)
  - [FreeDir](../F/FreeDir.md) (close directory descriptors)
  - [get_dirent_type](../g/get_dirent_type.md) (determine if entry is file or directory)
  - PGFILETYPE_DIR (constant for directory type)
  - MAX_TZDIR_DEPTH (maximum directory nesting depth)
  - [AllocateDir](../A/AllocateDir.md) (open directory for reading)
  - [tzload](../t/tzload.md) (load timezone data from file)
  - [pg_tz_acceptable](pg_tz_acceptable.md) (validate timezone for PostgreSQL use)
  - [strlcpy](../s/strlcpy.md) (safe string copying)
- Called from (representative examples):
  - [pg_timezone_names](pg_timezone_names.md) (in datetime.c for timezone enumeration loops)

## Notes and Other Information
- Returns NULL when no more timezones are available
- Implements depth-first traversal of the timezone directory tree
- Automatically skips hidden files (those starting with '.')
- Guards against directory stack overflow with MAX_TZDIR_DEPTH limit
- Uses tzload() instead of pg_tzset() to avoid polluting the timezone cache
- Filters out leap-second zones and other unacceptable timezones
- Constructs canonical timezone names by removing the base directory prefix
- Maintains open directory handles for efficient iteration through large directory trees
- Part of the timezone enumeration API trilogy: start, next, and end functions