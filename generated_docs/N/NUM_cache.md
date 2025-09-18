# NUM_cache

## Location
src/backend/utils/adt/formatting.c: 5180 - 5237

## Overview
Main caching interface for numeric formatting that manages format string parsing, caching strategy, and memory allocation based on format string size.

## Definition
static FormatNode *NUM_cache(int len, NUMDesc *Num, text *pars_str, bool *shouldFree)

## Detailed Description
This function serves as the primary entry point for numeric format caching in PostgreSQL's formatting system. It implements a size-based caching strategy: format strings within the cache size limit (NUM_CACHE_SIZE) are processed through the cache system using NUM_cache_fetch(), while oversized format strings bypass the cache entirely and are parsed directly into dynamically allocated memory. For cached formats, it copies the parsed NUMDesc structure from the cache entry to the output parameter. For non-cached formats, it performs direct parsing using parse_format(). The function manages memory allocation responsibility through the shouldFree parameter, indicating whether the caller must free the returned FormatNode array.

## Parameters / Member Variables
- : Length of the format string to determine caching strategy
- : Output parameter for numeric format descriptor that receives parsed format metadata
- : PostgreSQL text object containing the format picture string
- : Output parameter indicating if caller should free the returned FormatNode array

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - NUM_cache_fetch
  - zeroize_NUM
  - parse_format
  - palloc
  - pfree
  - NUM_CACHE_SIZE (constant)
  - NUM_keywords (global array)
  - NUM_index (global array)
  - NUM_FLAG (constant)
  - NUMCacheEntry (struct type)
  - FormatNode (struct type)
- Called from (representative examples):
  - NUM_TOCHAR_prepare
  - numeric_to_number

## Notes and Other Information
- Implements hybrid caching strategy based on format string length
- Format strings exceeding NUM_CACHE_SIZE bypass cache for memory efficiency
- Copies all nine NUMDesc fields from cache entry to output parameter (pre, post, lsign, flag, pre_lsign_num, need_locale, multi, zero_start, zero_end)
- Memory management varies by path: cached formats reuse cache memory, oversized formats require caller cleanup
- Central performance optimization point for PostgreSQL numeric formatting operations
- Debug logging available when DEBUG_TO_FROM_CHAR is defined
- Part of the numeric formatting cache management system located in src/backend/utils/adt/formatting.c:5180-5237