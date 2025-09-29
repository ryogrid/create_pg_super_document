# CatCachePrintStats

## Location
[src/backend/utils/cache/catcache.c:460-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L460-L527)

## Overview
CatCachePrintStats is a static function that outputs detailed statistics about PostgreSQL's catalog cache system performance to the debug log.

## Definition

```c
static void
CatCachePrintStats(int code, Datum arg)
```
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
  - [InitCatCache](../I/InitCatCache.md) (registered as exit callback)

## Notes and Other Information
- Function is declared as static, making it internal to the catcache.c module
- Uses DEBUG2 log level, so output is only visible when log_min_messages is set appropriately
- The function signature matches the PGProcAtExit callback prototype, allowing it to be registered as an exit handler
- Provides valuable performance tuning information for database administrators analyzing cache effectiveness
- Statistics include both successful cache operations and cache misses to give a complete performance picture

## Simplified Source

```c
// Simplified version of CatCachePrintStats
static void
CatCachePrintStats(int code, Datum arg)
{
    slist_iter iter;

    // Initialize totals for system-wide statistics
    long total_searches = 0, total_hits = 0, total_neg_hits = 0;
    long total_newloads = 0, total_invals = 0, total_nlists = 0;
    long total_lsearches = 0, total_lhits = 0;

    // Iterate through all catalog caches
    slist_foreach(iter, &CacheHdr->ch_caches)
    {
        CatCache *cache = slist_container(CatCache, cc_next, iter.cur);

        // Skip unused caches (no tuples and no searches)
        if (cache->cc_ntup == 0 && cache->cc_searches == 0)
            continue;

        // Print detailed statistics for this cache
        elog(DEBUG2, "catcache %s/%u: %d tup, %ld srch, %ld+%ld=%ld hits, %ld+%ld=%ld loads, %ld invals, %d lists, %ld lsrch, %ld lhits",
             cache->cc_relname, cache->cc_indexoid, cache->cc_ntup,
             cache->cc_searches, cache->cc_hits, cache->cc_neg_hits,
             cache->cc_hits + cache->cc_neg_hits, cache->cc_newloads,
             cache->cc_searches - cache->cc_hits - cache->cc_neg_hits - cache->cc_newloads,
             cache->cc_searches - cache->cc_hits - cache->cc_neg_hits,
             cache->cc_invals, cache->cc_nlist, cache->cc_lsearches, cache->cc_lhits);

        // Accumulate totals
        total_searches += cache->cc_searches;
        total_hits += cache->cc_hits;
        total_neg_hits += cache->cc_neg_hits;
        total_newloads += cache->cc_newloads;
        total_invals += cache->cc_invals;
        total_nlists += cache->cc_nlist;
        total_lsearches += cache->cc_lsearches;
        total_lhits += cache->cc_lhits;
    }

    // Print system-wide totals
    elog(DEBUG2, "catcache totals: %d tup, %ld srch, %ld+%ld=%ld hits, %ld+%ld=%ld loads, %ld invals, %ld lists, %ld lsrch, %ld lhits",
         CacheHdr->ch_ntup, total_searches, total_hits, total_neg_hits,
         total_hits + total_neg_hits, total_newloads,
         total_searches - total_hits - total_neg_hits - total_newloads,
         total_searches - total_hits - total_neg_hits,
         total_invals, total_nlists, total_lsearches, total_lhits);
}
```

Key simplifications made:
- Renamed cryptic variable names (cc_searches → total_searches, etc.) for clarity
- Added descriptive comments for each major section
- Grouped variable declarations logically at the top
- Simplified the complex elog expressions by using more readable variable names
- Preserved the essential algorithm: iterate caches, skip unused ones, print stats, accumulate totals
- Maintained the exact same functionality while improving readability