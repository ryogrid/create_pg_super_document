# init_timezone_hashtable

## Location
[src/timezone/pgtz.c:202-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/pgtz.c#L202-L233)

## Overview
Initializes the global hashtable used for caching timezone data structures to improve lookup performance.

## Definition
static bool init_timezone_hashtable(void)

## Detailed Description
This function creates and configures the global timezone_cache hashtable that stores parsed timezone data structures. The hashtable is designed to cache pg_tz_cache entries using timezone name strings as keys. It uses PostgreSQL's hash table infrastructure with string keys and a fixed entry size. The hashtable is configured with an initial size of 4 buckets and uses both HASH_ELEM (for custom element size) and HASH_STRINGS (for string key handling) flags. This caching mechanism significantly improves performance by avoiding repeated parsing of timezone files for the same timezone names.

## Parameters / Member Variables
This function takes no parameters and returns a boolean indicating success or failure of hashtable creation.

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md) (creates the hashtable with specified configuration)
  - [HASHCTL](../H/HASHCTL.md) (hashtable control structure)
  - pg_tz_cache (timezone cache entry structure type)
  - TZ_STRLEN_MAX (maximum timezone string length constant)
  - HASH_ELEM (hashtable flag for custom element size)
  - HASH_STRINGS (hashtable flag for string key handling)
- Called from (representative examples):
  - [pg_tzset](../p/pg_tzset.md) (in src/timezone/pgtz.c:246)

## Notes and Other Information
- This is a static function, accessible only within the same source file
- The hashtable is initialized with 4 initial buckets, which will grow as needed
- Uses string-based keys with maximum length of TZ_STRLEN_MAX + 1 characters
- Returns true on successful initialization, false if hashtable creation fails
- The created hashtable is stored in the global timezone_cache variable
- This function should be called only once during timezone subsystem initialization
- Location: src/timezone/pgtz.c:202-233

## Simplified Source

```c
// Simplified version of init_timezone_hashtable
static bool init_timezone_hashtable(void) {
    HASHCTL hash_config;

    // Configure hashtable parameters
    hash_config.keysize = TZ_STRLEN_MAX + 1;     // Max timezone name length + null terminator
    hash_config.entrysize = sizeof(pg_tz_cache); // Size of cached timezone entries

    // Create the global timezone cache hashtable
    timezone_cache = hash_create("Timezones",           // Table name for debugging
                                4,                       // Initial bucket count
                                &hash_config,            // Configuration struct
                                HASH_ELEM | HASH_STRINGS); // Use custom entry size + string keys

    // Return success/failure status
    if (!timezone_cache) {
        return false;  // Hashtable creation failed
    }

    return true;  // Successfully initialized
}
```

Key simplifications made:
- Added descriptive comments explaining each configuration parameter
- Clarified the purpose of each hash_create argument
- Made the success/failure logic more explicit with comments
- Used more descriptive variable name (hash_config instead of hash_ctl)
- Maintained the exact same functionality while improving readability