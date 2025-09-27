# pg_tzset

## Location
[src/timezone/pgtz.c:234-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/pgtz.c#L234-L319)

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
  - [tzparse](../t/tzparse.md) (parses POSIX timezone specifications)
  - [tzload](../t/tzload.md) (loads timezone from filesystem)
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

## Simplified Source

```c
// Simplified version of pg_tzset
pg_tz *pg_tzset(const char *tzname) {
    pg_tz_cache *cache_entry;
    struct state timezone_state;
    char uppercase_name[TZ_STRLEN_MAX + 1];
    char canonical_name[TZ_STRLEN_MAX + 1];
    char *p;

    // Check timezone name length
    if (strlen(tzname) > TZ_STRLEN_MAX)
        return NULL;

    // Initialize cache if needed
    if (!timezone_cache) {
        if (!init_timezone_hashtable())
            return NULL;
    }

    // Convert timezone name to uppercase for case-insensitive lookup
    p = uppercase_name;
    while (*tzname)
        *p++ = pg_toupper((unsigned char) *tzname++);
    *p = '\0';

    // Check if timezone is already cached
    cache_entry = hash_search(timezone_cache, uppercase_name, HASH_FIND, NULL);
    if (cache_entry) {
        return &cache_entry->tz;  // Return cached timezone
    }

    // Handle "GMT" specially - always use tzparse for reliability
    if (strcmp(uppercase_name, "GMT") == 0) {
        if (!tzparse(uppercase_name, &timezone_state, true)) {
            elog(ERROR, "could not initialize GMT time zone");
        }
        strcpy(canonical_name, uppercase_name);
    }
    // Try to load timezone from filesystem
    else if (tzload(uppercase_name, canonical_name, &timezone_state, true) != 0) {
        // If filesystem load fails, try parsing as POSIX timezone spec
        if (uppercase_name[0] == ':' || !tzparse(uppercase_name, &timezone_state, false)) {
            return NULL;  // Unknown timezone
        }
        strcpy(canonical_name, uppercase_name);
    }

    // Cache the loaded timezone for future use
    cache_entry = hash_search(timezone_cache, uppercase_name, HASH_ENTER, NULL);
    strcpy(cache_entry->tz.TZname, canonical_name);
    memcpy(&cache_entry->tz.state, &timezone_state, sizeof(timezone_state));

    return &cache_entry->tz;
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Added descriptive comments for each major logic step
- Simplified the name conversion loop structure
- Clarified the special GMT handling logic
- Made the filesystem vs POSIX timezone parsing flow more readable
- Emphasized the caching mechanism with clear comments
- Focused on the main execution paths without losing essential functionality