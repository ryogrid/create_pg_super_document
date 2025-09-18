# NUM_prevent_counter_overflow

## Location
src/backend/utils/adt/formatting.c: 5060 - 5071

## Overview
Prevents integer overflow in the NUMCounter aging mechanism by resetting cache entry ages when the counter approaches INT_MAX.

## Definition
static inline void NUM_prevent_counter_overflow(void)

## Detailed Description
This function implements a counter overflow prevention mechanism for the numeric formatting cache system in PostgreSQL. When the global aging counter NUMCounter approaches the maximum integer value (INT_MAX - 1), it prevents potential overflow by performing a coordinated reset operation. The function halves both the NUMCounter and the age values of all cached entries using right-shift operations, effectively preserving the relative age ordering while keeping the values within safe integer bounds. This design maintains cache aging behavior while preventing arithmetic overflow that could corrupt the cache management system.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - NUMCounter (global variable)
  - NUMCache (global array)
  - n_NUMCache (global variable)
  - NUMCacheEntry (struct type)
- Called from (representative examples):
  - [NUM_cache_getnew](NUM_cache_getnew.md)
  - [NUM_cache_search](NUM_cache_search.md)

## Notes and Other Information
- This function works similarly to DCH_prevent_counter_overflow for date/time formatting cache
- Uses inline function for performance optimization since it's called frequently during cache operations
- The right-shift operation (>>= 1) is used for efficient division by 2
- Critical for maintaining cache integrity in long-running PostgreSQL sessions where many numeric formatting operations occur
- Part of the numeric formatting cache management system located in src/backend/utils/adt/formatting.c:5060-5071