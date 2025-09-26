# tzloadbody

## Location
src/timezone/localtime.c: 211 - 585

## Overview
Loads timezone data from a timezone database file into a timezone state structure, parsing both the binary timezone data format and optional POSIX timezone strings.

## Definition


## Detailed Description
The `tzloadbody` function is the core timezone file parser in PostgreSQL's timezone system. It reads and validates timezone data files (tzfile format), parsing binary data that includes transition times, timezone types, leap second information, and timezone abbreviations.

The function supports both 32-bit and 64-bit timestamp formats, and can handle extended POSIX timezone strings for future transitions beyond the file's explicit data. It performs extensive validation of the data and applies various compatibility workarounds for older timezone database formats.

The function processes timezone files in multiple passes to handle both 32-bit (4-byte) and 64-bit (8-byte) timestamp formats, discarding transitions outside the representable time range and optimizing the data for efficient lookups.

## Parameters / Member Variables
- `name`: Name of the timezone file to load (NULL defaults to TZDEFAULT)
- `canonname`: Buffer to store the canonical name of the timezone (must be > TZ_STRLEN_MAX bytes, can be NULL)
- `sp`: Pointer to the state structure to populate with timezone data
- `doextend`: Whether to process extended POSIX timezone strings for future transitions
- `lsp`: Temporary storage for file I/O and parsing operations

## Dependencies
- Functions called/Symbols referenced:
  - pg_open_tzfile (opens timezone files)
  - detzcode, detzcode64 (decode big-endian integers)
  - differ_by_repeat (check for repeating patterns)
  - typesequiv (check timezone type equivalence)
  - tzparse (parse POSIX timezone strings)
  - leapcorr (leap second corrections)
- Called from (representative examples):
  - tzload (single caller at line 594)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c compilation unit
- Supports multiple file format versions and handles both 32-bit and 64-bit timestamps
- Includes extensive validation of timezone file data to prevent malformed input
- Implements compatibility workarounds for timezone data from different eras (pre-2013c, 2018e, etc.)
- Optimizes timezone transitions by detecting and utilizing repeating patterns (400-year cycles)
- Handles leap second data with validation for proper timing and corrections
- Can extend timezone data with POSIX timezone strings for transitions beyond the file's explicit data
- Returns 0 on success, or an errno value on failure (EINVAL, ENOENT, etc.)
- The function is quite large (375 lines) due to the complexity of the timezone file format and various edge cases
- Includes special handling for timezone abbreviation reuse to minimize memory usage