# pg_tzset

## Location
src/timezone/pgtz.c: 234 - 319

## Overview
Loads a timezone definition from file or cache, providing the main interface for obtaining timezone data structures in PostgreSQL.

## Definition
pg_tz *pg_tzset(const char *tzname)

## Detailed Description
This function serves as the primary interface for loading timezone information in PostgreSQL. It first attempts to find the timezone in the cache using a case-insensitive lookup. If not found, it loads the timezone from the filesystem or parses it as a POSIX timezone specification. The function has special handling for "GMT" which is always processed via tzparse() without filesystem access to ensure reliability during bootstrap and avoid potential issues with leap-second-aware versions. The function converts the input timezone name to uppercase for consistent hashtable operations and caching. If the timezone is successfully loaded, it's cached for future use and a pointer to the timezone structure is returned.

## Parameters / Member Variables
- `tzname`: The timezone name to load (e.g., "America/New_York", "GMT", "PST8PDT")

## Dependencies
- Functions called/Symbols referenced:
  - [init_timezone_hashtable](../i/init_timezone_hashtable.md) (initializes cache if not already done)
  - [pg_toupper](pg_toupper.md) (converts timezone name to uppercase)
  - [hash_search](../h/hash_search.md) (searches and inserts entries in timezone cache)
  - tzparse (parses POSIX timezone specifications)
  - tzload (loads timezone from filesystem)
  - TZ_STRLEN_MAX (maximum timezone string length)
  - pg_tz_cache (cache entry structure type)
  - HASH_FIND and HASH_ENTER (hashtable operation flags)
- Called from (representative examples):
  - [check_timezone](../c/check_timezone.md) (src/backend/commands/variable.c:341)
  - [DecodeDateTime](../D/DecodeDateTime.md) (src/backend/utils/adt/datetime.c:1120, 1479)
  - [DecodeTimeOnly](../D/DecodeTimeOnly.md) (src/backend/utils/adt/datetime.c:1959, 2251)
  - [pg_timezone_initialize](pg_timezone_initialize.md) (src/timezone/pgtz.c:370)

## Notes and Other Information
- Returns NULL if the timezone name is too long (> TZ_STRLEN_MAX) or cannot be loaded
- "GMT" is handled specially - always parsed via tzparse() for reliability and speed
- Performs case-insensitive timezone lookups by converting names to uppercase
- Implements a caching mechanism to avoid repeated filesystem access for the same timezone
- Does not verify that the loaded timezone is acceptable - that's left to calling code
- Supports both filesystem-based timezone files and POSIX timezone specifications
- The cache is automatically initialized on first use if not already present
- Location: src/timezone/pgtz.c:234-319