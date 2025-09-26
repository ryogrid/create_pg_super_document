# DCH_cache_fetch

## Location
[src/backend/utils/adt/formatting.c:4154-4180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L4154-L4180)

## Overview
High-level cache interface that finds an existing parsed format entry or creates and parses a new one, returning a valid DCHCacheEntry ready for use.

## Definition
```c
static DCHCacheEntry *DCH_cache_fetch(const char *str, bool std)
```

## Detailed Description
This function serves as the primary interface to the DCH (Date/Time formatting Cache) system, implementing a complete cache-or-create pattern. It coordinates the entire cache lifecycle through a two-phase operation:

**Phase 1 - Cache Lookup**: The function first attempts to find an existing cache entry using DCH_cache_search(). If a matching entry is found (same format string and standard flag), it returns the cached, pre-parsed format immediately.

**Phase 2 - Cache Miss Handling**: When no matching entry exists, the function:
1. Obtains a new cache entry via DCH_cache_getnew()
2. Parses the format string using parse_format() with appropriate DCH formatting rules
3. Sets the standard flag (STD_FLAG) if the std parameter is true
4. Marks the entry as valid, making it available for future cache lookups

The parsing operation uses PostgreSQL's comprehensive date/time format parsing system, including DCH_keywords, DCH_suff, and DCH_index arrays. The DCH_FLAG ensures proper date/time formatting behavior, while the optional STD_FLAG enables standard SQL format interpretation.

## Parameters / Member Variables
- `str`: The date/time format pattern string to be cached and parsed
- `std`: Boolean flag indicating whether to use standard SQL format interpretation (adds STD_FLAG during parsing)

## Dependencies
- Functions called/Symbols referenced:
  - DCH_cache_search (cache lookup)
  - DCH_cache_getnew (cache entry allocation)
  - parse_format (format string parser)
  - DCH_keywords (format keyword array)
  - DCH_suff (format suffix array)  
  - DCH_index (format index array)
  - DCH_FLAG (date/time format flag)
  - STD_FLAG (standard format flag)
- Called from:
  - datetime_to_char_body (datetime to string conversion)
  - datetime_format_has_tz (timezone detection)
  - do_to_timestamp (string to timestamp conversion)

## Notes and Other Information
- Guarantees that returned entries are always valid and ready for immediate use
- Implements lazy parsing - formats are only parsed when first accessed
- The standard flag creates separate cache entries for standard vs non-standard interpretations
- Critical performance optimization for repeated use of the same format strings
- Forms the public interface of the three-function cache management system
- Parse failures would leave cache entries invalid, preventing future incorrect lookups
- Thread-safe due to the single-threaded nature of PostgreSQL backend processes