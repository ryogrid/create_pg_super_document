# pg_stat_reset_slru

## Location
[src/backend/utils/adt/pgstatfuncs.c:1772-1788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pgstatfuncs.c#L1772-L1788)

## Overview
A PostgreSQL system function that resets Simple LRU (SLRU) cache statistics, allowing selective reset of a specific SLRU cache or all SLRU caches when no target is specified.

## Definition

```c
Datum
pg_stat_reset_slru(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides a mechanism to reset statistics for PostgreSQL's Simple LRU (SLRU) caches. SLRU caches are used throughout PostgreSQL to manage various types of cached data, including transaction status, commit timestamps, multixact information, and other frequently accessed metadata.

The function accepts an optional text parameter that specifies which SLRU cache to reset. When called without arguments (NULL), it resets statistics for all SLRU caches by calling  with . When a specific target is provided, it calls the more specific  function to reset statistics for the named SLRU cache.

## Parameters / Member Variables
-  (optional text): Specifies which SLRU cache statistics to reset. When NULL, all SLRU statistics are reset. When provided, it should be the name of a specific SLRU cache (such as "CommitTs", "MultiXactOffset", "MultiXactMember", etc.).

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_reset_of_kind](pgstat_reset_of_kind.md) (to reset all SLRU statistics)
  - text_to_cstring (to convert text parameter to C string)
  - [pgstat_reset_slru](pgstat_reset_slru.md) (to reset specific SLRU cache statistics)
  - PG_RETURN_VOID (to return from the function)
- Constants used:
  - PGSTAT_KIND_SLRU (specifies SLRU statistics category)
- Called from:
  - SQL function interface (no direct C callers found)

## Notes and Other Information
- SLRU (Simple LRU) caches are fundamental components of PostgreSQL's memory management system
- Common SLRU caches include those for transaction commit timestamps, multixact data, subtransaction information, and notification queues
- The function provides flexibility by allowing both complete reset (all SLRU caches) and selective reset (specific named cache)
- SLRU statistics typically include metrics like cache hits, cache misses, page reads, page writes, and cache evictions
- This function complements the broader  function, which can also reset SLRU statistics as part of its "slru" target option
- The function requires appropriate administrative privileges to execute, as SLRU statistics are cluster-wide resources
- Invalid SLRU cache names will be handled by the underlying  function