# scan_directory_ci

## Location
[src/timezone/pgtz.c:151-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/pgtz.c#L151-L195)

## Overview
Scans a specified directory for a case-insensitive match to a given filename and returns the actual (canonical) filename if found.

## Definition
static bool scan_directory_ci(const char *dirname, const char *fname, int fnamelen, char *canonname, int canonnamelen)

## Detailed Description
This function performs a case-insensitive directory scan to locate a file matching the provided filename. It iterates through all entries in the specified directory and compares each entry's name with the target filename using case-insensitive comparison. If a match is found, the function copies the actual filename (with correct casing) into the canonname buffer and returns true. The function includes security measures by ignoring hidden files (those starting with '.') to prevent access to files outside the intended timezone directory scope.

## Parameters / Member Variables
- `dirname`: The directory path to scan for the matching file
- `fname`: The target filename to search for (may not be null-terminated)
- `fnamelen`: The length of the fname string
- `canonname`: Output buffer to store the canonical (actual) filename if found
- `canonnamelen`: The size of the canonname buffer

## Dependencies
- Functions called/Symbols referenced:
  - AllocateDir (allocates directory descriptor)
  - ReadDirExtended (reads directory entries with error logging)
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (case-insensitive string comparison)
  - strlcpy (safe string copying)
  - FreeDir (frees directory descriptor)
  - DIR (directory structure type)
  - [dirent](../d/dirent.md) (directory entry structure)
- Called from (representative examples):
  - [pg_open_tzfile](../p/pg_open_tzfile.md) (in src/timezone/pgtz.c:126)

## Notes and Other Information
- This is a static function, accessible only within the same source file
- Implements security measures by skipping hidden files (starting with '.')
- Designed specifically for timezone file lookups where case-insensitive matching is required
- Returns true if a match is found, false otherwise
- The fname parameter doesn't need to be null-terminated since fnamelen specifies its length
- Uses PostgreSQL's directory handling functions for consistent error handling and logging
- Location: src/timezone/pgtz.c:151-195