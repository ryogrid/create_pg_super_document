# DCHCacheEntry

## Location
src/backend/utils/adt/formatting.c: 375 - 383

## Overview
A cache entry structure for storing parsed date/time format templates to avoid repeated parsing of frequently used format strings in PostgreSQL's formatting functions.

## Definition


## Detailed Description
DCHCacheEntry is a cache structure used to store parsed date/time format templates in PostgreSQL's formatting system. When functions like , , or  are called with format strings, the system parses these strings into FormatNode arrays. To avoid expensive re-parsing of frequently used format strings, the parsed results are cached in DCHCacheEntry structures.

The cache operates as a least-recently-used (LRU) replacement policy using an aging mechanism. Each entry tracks its usage through an age counter, and when the cache is full, the oldest (highest age) entries are replaced. The cache has a fixed size of DCH_CACHE_ENTRIES (20) entries.

The cache only stores format strings up to DCH_CACHE_SIZE length. Longer format strings are parsed dynamically each time without caching.

## Parameters / Member Variables
- : Array of FormatNode structures representing the parsed format template, sized to accommodate maximum cacheable format length plus null terminator
- : Copy of the original format string for lookup purposes, null-terminated
- : Boolean flag indicating whether this is a "standard" format (affects parsing behavior, particularly error handling)
- : Boolean flag indicating whether this cache entry contains valid data and can be used
- : Integer counter used for LRU replacement policy - incremented on each cache access, entries with highest age are replaced first

## Dependencies
- Functions called/Symbols referenced:
  - FormatNode structure (for parsed format storage)
  - DCH_CACHE_SIZE constant (maximum cacheable format length)
- Called from (representative examples):
  - DCH_cache_getnew: Creates new cache entries
  - DCH_cache_search: Searches for existing cache entries
  - DCH_cache_fetch: Main cache access function
  - datetime_to_char_body: Uses cached format templates
  - do_to_timestamp: Uses cached format templates
  - datetime_format_has_tz: Uses cached format templates

## Notes and Other Information
- Part of a global cache system with DCHCache array containing up to 20 entries
- Cache management includes aging mechanism where all ages are right-shifted when DCHCounter overflows to prevent integer overflow
- Only format strings shorter than DCH_CACHE_SIZE are cached; longer ones are processed without caching
- The 'std' flag distinguishes between standard and non-standard format parsing modes, affecting error reporting behavior
- Cache entries are allocated in TopMemoryContext to persist across query executions
- Companion structure NUMCacheEntry serves similar purpose for numeric formatting
- Cache dramatically improves performance for applications using the same format strings repeatedly