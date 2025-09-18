# CatCachePrintStats

## Location
src/backend/utils/cache/catcache.c: 460 - 527

## Overview
CatCachePrintStats is a static function that outputs detailed statistics about PostgreSQL's catalog cache system performance to the debug log.

## Definition


## Detailed Description
This function serves as a debugging utility that iterates through all catalog caches in the system and prints comprehensive performance statistics for each active cache. It's designed to be registered as an exit callback to provide insights into cache usage patterns during database shutdown or when explicitly triggered.

The function calculates and displays both individual cache statistics and system-wide totals, including:
- Number of tuples stored in each cache
- Search operations performed
- Cache hits (both positive and negative)
- New loads from disk
- Cache invalidations
- List operations and their hit rates

Statistics are only printed for caches that have been actively used (non-zero tuple count or search operations).

## Parameters / Member Variables
- : Exit code parameter (unused in the function body)
- : Datum argument (unused in the function body)

## Dependencies
- Functions called/Symbols referenced:
  - slist_foreach (macro for iterating through cache list)
  - slist_container (macro to get cache structure from list node)
  - elog (for debug output)
- Called from (representative examples):
  - InitCatCache (registered as exit callback)

## Notes and Other Information
- Function is declared as static, making it internal to the catcache.c module
- Uses DEBUG2 log level, so output is only visible when log_min_messages is set appropriately
- The function signature matches the PGProcAtExit callback prototype, allowing it to be registered as an exit handler
- Provides valuable performance tuning information for database administrators analyzing cache effectiveness
- Statistics include both successful cache operations and cache misses to give a complete performance picture