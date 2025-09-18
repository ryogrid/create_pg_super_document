# pg_load_tz

## Location
src/bin/initdb/findtimezone.c: 91 - 151

## Overview
Loads a timezone definition into memory for use during database initialization, supporting both timezone file-based and POSIX timezone string formats.

## Definition


## Detailed Description
The pg_load_tz function is responsible for loading timezone definitions during the initdb process. It serves as a simplified version of the backend's pg_tzset() function, with the key limitation that it only supports one loaded timezone at a time using a static pg_tz structure.

The function handles timezone loading through multiple mechanisms:
1. **GMT Special Case**: The "GMT" timezone is always handled through tzparse() for consistency
2. **File-based Timezones**: Most timezone names are loaded using tzload(), which reads binary timezone data files
3. **POSIX Timezone Strings**: If file loading fails, it attempts to parse the name as a POSIX timezone string using tzparse()

The function performs basic validation on the timezone name length but does not verify that the loaded timezone is acceptable for use - that responsibility lies with the caller.

## Parameters / Member Variables
- : The timezone name to load (e.g., "America/New_York", "GMT", or a POSIX timezone string like "EST5EDT")

## Dependencies
- Functions called/Symbols referenced:
  - pg_tz (timezone structure type)
  - TZ_STRLEN_MAX (constant for maximum timezone name length)
  - tzparse (parses POSIX timezone strings)
  - tzload (loads timezone data from files)
- Called from (representative examples):
  - score_timezone (in src/bin/initdb/findtimezone.c)
  - validate_zone (in src/bin/initdb/findtimezone.c)

## Notes and Other Information
- Returns a pointer to a static pg_tz structure on success, NULL on failure
- Only one timezone can be loaded at a time due to the use of a static structure
- The function does not perform timezone acceptability validation - callers must handle this
- Special handling for "GMT" ensures consistent behavior with the backend pg_tzset()
- Supports both binary timezone files and POSIX timezone string formats
- Names starting with ':' are treated as file references and will not be parsed as POSIX strings
- This is a static function, only accessible within the findtimezone.c file
- The loaded timezone information includes both the timezone state data and the original timezone name