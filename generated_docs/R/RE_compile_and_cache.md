# RE_compile_and_cache

## Location
[src/backend/utils/adt/regexp.c:141-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L141-L281)

## Overview
Compiles a regular expression and caches it for reuse, implementing an LRU cache to improve performance by avoiding repeated compilation of the same patterns.

## Definition

```c
structure is self-organizing with most-used entries at the front, our
	 * search strategy can just be to scan from the front.
	 */
	for (i = 0;
```
## Detailed Description
This function provides a caching layer for regular expression compilation in PostgreSQL. It maintains an internal cache of compiled regular expressions (regex_t structures) with an LRU (Least Recently Used) replacement policy. When a regex pattern is requested, it first searches the cache for an existing compiled version with matching pattern, flags, and collation. If found, it moves the entry to the front of the cache and returns it. If not found, it compiles the new pattern, stores it in the cache (potentially evicting the oldest entry if the cache is full), and returns the compiled regex.

The function handles memory management carefully by creating separate memory contexts for each cached regex to prevent memory leaks and enable proper cleanup. It converts the input text pattern from the database encoding to wide characters (pg_wchar) as required by Spencer's regex library.

## Parameters / Member Variables
- : The regular expression pattern as a TEXT object in database encoding
- : Compilation flags that control regex behavior (case sensitivity, etc.)
- : The collation OID to use for LC_CTYPE-dependent behavior during compilation

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE_ANY_EXHDR, VARDATA_ANY (text handling macros)
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md) (character encoding conversion)
  - AllocSetContextCreate (memory context creation)
  - pg_regcomp (regex compilation)
  - [pg_regerror](../p/pg_regerror.md) (error message generation)
  - [MemoryContextSetIdentifier](../M/MemoryContextSetIdentifier.md), MemoryContextSetParent, MemoryContextDelete (memory management)
  - [cached_re_str](../c/cached_re_str.md) (cache entry structure)
  - MAX_CACHED_RES (cache size constant)
- Called from (representative examples):
  - [RE_compile_and_execute](RE_compile_and_execute.md)
  - [textregexsubstr](../t/textregexsubstr.md)
  - [setup_regexp_matches](../s/setup_regexp_matches.md)
  - [regexp_fixed_prefix](../r/regexp_fixed_prefix.md)
  - [replace_text_regexp](../r/replace_text_regexp.md)

## Notes and Other Information
- Implements an LRU cache with a maximum size defined by MAX_CACHED_RES
- Uses a self-organizing data structure where frequently used entries migrate to the front
- Creates individual memory contexts for each cached regex to enable proper cleanup
- Handles compilation errors by throwing PostgreSQL ERRORs with appropriate error codes
- The cache is global and persists across function calls within a backend process
- Thread-safe within PostgreSQL's single-threaded backend model

## Simplified Source

```c
regex_t *RE_compile_and_cache(text *text_re, int cflags, Oid collation) {
    int text_len = VARSIZE_ANY_EXHDR(text_re);
    char *text_val = VARDATA_ANY(text_re);

    // Search existing cache (LRU - most recent at front)
    for (int i = 0; i < num_res; i++) {
        if (re_array[i].cre_pat_len == text_len &&
            re_array[i].cre_flags == cflags &&
            re_array[i].cre_collation == collation &&
            memcmp(re_array[i].cre_pat, text_val, text_len) == 0) {

            // Move to front if not already there (LRU update)
            if (i > 0) {
                cached_re_str temp = re_array[i];
                memmove(&re_array[1], &re_array[0], i * sizeof(cached_re_str));
                re_array[0] = temp;
            }
            return &re_array[0].cre_re;
        }
    }

    // Initialize cache memory context if needed
    if (unlikely(RegexpCacheMemoryContext == NULL)) {
        RegexpCacheMemoryContext = AllocSetContextCreate(TopMemoryContext,
                                                         "RegexpCacheMemoryContext",
                                                         ALLOCSET_SMALL_SIZES);
    }

    // Compile new regex
    cached_re_str new_entry;

    // Convert to wide characters for regex library
    pg_wchar *pattern = palloc((text_len + 1) * sizeof(pg_wchar));
    int pattern_len = pg_mb2wchar_with_len(text_val, pattern, text_len);

    // Create memory context for this regex
    new_entry.cre_context = AllocSetContextCreate(CurrentMemoryContext,
                                                  "RegexpMemoryContext",
                                                  ALLOCSET_SMALL_SIZES);
    MemoryContext oldcontext = MemoryContextSwitchTo(new_entry.cre_context);

    // Compile the regex
    int result = pg_regcomp(&new_entry.cre_re, pattern, pattern_len, cflags, collation);
    pfree(pattern);

    if (result != REG_OKAY) {
        char errMsg[100];
        pg_regerror(result, &new_entry.cre_re, errMsg, sizeof(errMsg));
        ereport(ERROR, (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
                       errmsg("invalid regular expression: %s", errMsg)));
    }

    // Store pattern in regex memory context
    new_entry.cre_pat = palloc(text_len + 1);
    memcpy(new_entry.cre_pat, text_val, text_len);
    new_entry.cre_pat[text_len] = 0;  // Null terminate for context identifier
    new_entry.cre_pat_len = text_len;
    new_entry.cre_flags = cflags;
    new_entry.cre_collation = collation;

    // Insert into cache (evict oldest if full)
    if (num_res >= MAX_CACHED_RES) {
        MemoryContextDelete(re_array[--num_res].cre_context);
    }

    // Move existing entries down, insert new one at front
    if (num_res > 0) {
        memmove(&re_array[1], &re_array[0], num_res * sizeof(cached_re_str));
    }

    // Re-parent context to cache and insert
    MemoryContextSetParent(new_entry.cre_context, RegexpCacheMemoryContext);
    re_array[0] = new_entry;
    num_res++;

    MemoryContextSwitchTo(oldcontext);
    return &re_array[0].cre_re;
}
```