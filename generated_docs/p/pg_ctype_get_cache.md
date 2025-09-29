# pg_ctype_get_cache

## Location
[src/backend/regex/regc_pg_locale.c:766-922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L766-L922)

## Overview
Builds and caches character class vectors (cvec) for character classification functions, scanning character ranges to determine which characters satisfy a given probe function.

## Definition
```c
static struct cvec *pg_ctype_get_cache(pg_wc_probefunc probefunc, int cclasscode)
```

## Detailed Description
This function creates and manages cached character class vectors for regex character classification. It takes a probe function (like pg_wc_isalpha or pg_wc_isspace) and scans through the character space to build a comprehensive list of characters that satisfy that function. The results are cached to avoid expensive recomputation.

The function implements a sophisticated caching mechanism that considers both the probe function and the current collation. It scans characters from 0 to a strategy-dependent maximum, grouping consecutive matching characters into ranges for efficient storage. The scanning range is optimized based on the regex strategy - for example, C locale only needs to scan up to 127, while other strategies may scan the full MAX_SIMPLE_CHR range.

Memory management is carefully handled with dynamic allocation and cleanup on failure. The function optimizes storage by consolidating consecutive characters into ranges and trimming unused allocated space.

## Parameters / Member Variables
- `probefunc`: Function pointer to the character classification function (e.g., pg_wc_isalpha, pg_wc_isspace)
- `cclasscode`: Integer code identifying the character class type

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [store_match](../s/store_match.md)
  - realloc
  - free
- Types referenced:
  - [pg_ctype_cache](pg_ctype_cache.md)
  - [cvec](../c/cvec.md)
  - [chr](../c/chr.md)
  - pg_wc_probefunc
- Constants referenced:
  - MAX_SIMPLE_CHR
  - PG_REGEX_LOCALE_C
  - PG_REGEX_BUILTIN
  - PG_REGEX_LOCALE_WIDE
  - PG_REGEX_LOCALE_WIDE_L
  - PG_REGEX_LOCALE_1BYTE
  - PG_REGEX_LOCALE_1BYTE_L
  - PG_REGEX_LOCALE_ICU
- Called from (representative examples):
  - [cclasscvec](../c/cclasscvec.md) (multiple times for different character classes)

## Notes and Other Information
- Returns a pointer to the cached cvec structure, or NULL if out of memory
- The returned cvec must not be freed or modified by the caller
- Implements a linked-list cache (pg_ctype_cache_list) to avoid recomputation
- Optimizes character scanning range based on regex strategy to avoid unnecessary work
- Groups consecutive matching characters into ranges for memory efficiency
- Marks cvec as locale-independent (-1 cclasscode) when scanning is limited by strategy constraints
- Handles memory allocation failures gracefully with proper cleanup
- Used extensively by the regex engine for character class operations like \\d, \\w, \\s, etc.
- The cache persists across regex operations within the same session

## Simplified Source

```c
static struct cvec *
pg_ctype_get_cache(pg_wc_probefunc probefunc, int cclasscode)
{
    pg_ctype_cache *cache_entry;
    pg_wchar max_char;
    pg_wchar current_char;
    int consecutive_matches;

    // Check if result already cached
    for (cache_entry = pg_ctype_cache_list; cache_entry != NULL; cache_entry = cache_entry->next) {
        if (cache_entry->probefunc == probefunc &&
            cache_entry->collation == pg_regex_collation) {
            return &cache_entry->cv;
        }
    }

    // Allocate new cache entry
    cache_entry = malloc(sizeof(pg_ctype_cache));
    if (cache_entry == NULL) return NULL;

    // Initialize cache entry
    cache_entry->probefunc = probefunc;
    cache_entry->collation = pg_regex_collation;
    cache_entry->cv.cclasscode = cclasscode;

    // Allocate initial space for characters and ranges
    cache_entry->cv.chrspace = 128;
    cache_entry->cv.chrs = malloc(cache_entry->cv.chrspace * sizeof(chr));
    cache_entry->cv.rangespace = 64;
    cache_entry->cv.ranges = malloc(cache_entry->cv.rangespace * sizeof(chr) * 2);

    if (cache_entry->cv.chrs == NULL || cache_entry->cv.ranges == NULL) {
        goto cleanup_and_fail;
    }

    // Determine scan range based on regex strategy
    switch (pg_regex_strategy) {
        case PG_REGEX_LOCALE_C:
            max_char = 127;
            cache_entry->cv.cclasscode = -1;  // Locale-independent
            break;
        case PG_REGEX_LOCALE_1BYTE:
        case PG_REGEX_LOCALE_1BYTE_L:
            max_char = UCHAR_MAX;
            cache_entry->cv.cclasscode = -1;  // Locale-independent
            break;
        default:
            max_char = MAX_SIMPLE_CHR;
            break;
    }

    // Scan characters and find matches
    consecutive_matches = 0;
    for (current_char = 0; current_char <= max_char; current_char++) {
        if ((*probefunc)(current_char)) {
            consecutive_matches++;
        } else if (consecutive_matches > 0) {
            // Store the range of matches
            store_match(cache_entry, current_char - consecutive_matches, consecutive_matches);
            consecutive_matches = 0;
        }
    }

    // Handle final consecutive matches at end
    if (consecutive_matches > 0) {
        store_match(cache_entry, current_char - consecutive_matches, consecutive_matches);
    }

    // Optimize memory usage by trimming unused space
    if (cache_entry->cv.nchrs < cache_entry->cv.chrspace) {
        cache_entry->cv.chrs = realloc(cache_entry->cv.chrs,
                                     cache_entry->cv.nchrs * sizeof(chr));
    }
    if (cache_entry->cv.nranges < cache_entry->cv.rangespace) {
        cache_entry->cv.ranges = realloc(cache_entry->cv.ranges,
                                       cache_entry->cv.nranges * sizeof(chr) * 2);
    }

    // Add to cache chain
    cache_entry->next = pg_ctype_cache_list;
    pg_ctype_cache_list = cache_entry;

    return &cache_entry->cv;

cleanup_and_fail:
    free(cache_entry->cv.chrs);
    free(cache_entry->cv.ranges);
    free(cache_entry);
    return NULL;
}
```