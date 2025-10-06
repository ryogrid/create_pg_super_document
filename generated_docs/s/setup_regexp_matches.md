# setup_regexp_matches

## Location
[src/backend/utils/adt/regexp.c:1442-1645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1442-L1645)

## Overview
Performs the initial pattern matching setup for regexp_match, regexp_matches, regexp_split, and related functions by compiling the regex pattern and finding all matches in the input string.

## Definition
```c
static regexp_matches_ctx *setup_regexp_matches(text *orig_str, text *pattern, pg_re_flags *re_flags,
                                               int start_search, Oid collation,
                                               bool use_subpatterns,
                                               bool ignore_degenerate,
                                               bool fetching_unmatched)
```

## Detailed Description
This function is the core regex matching engine for PostgreSQL's regular expression functions. It:

1. Converts the input string to wide character format for proper Unicode handling
2. Compiles the regex pattern with appropriate flags and caching
3. Performs all pattern matching in one operation to avoid recompilation overhead
4. Stores match locations and subpattern information in a context structure
5. Handles memory management for variable-length result arrays
6. Supports both global (all matches) and single-match modes
7. Manages character encoding conversions for multi-byte character sets

The function optimizes performance by doing all matching upfront and caching results, rather than re-executing the regex on each function call. It also handles degenerate (zero-length) matches and supports fetching unmatched portions for split operations.

## Parameters / Member Variables
- `orig_str`: The original input text string to search within
- `pattern`: The regular expression pattern to match against  
- `re_flags`: Compiled regex flags structure containing options like global matching
- `start_search`: Character offset in orig_str where matching should begin
- `collation`: Database collation to use for pattern matching
- `use_subpatterns`: Whether to collect data about parenthesized subexpression matches
- `ignore_degenerate`: Whether to ignore zero-length matches
- `fetching_unmatched`: Whether caller wants to fetch unmatched substring portions

## Dependencies
- Functions called/Symbols referenced:
  - [RE_compile_and_cache](../R/RE_compile_and_cache.md)
  - [RE_wchar_execute](../R/RE_wchar_execute.md)  
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md)
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md)
  - [repalloc](../r/repalloc.md), palloc, pfree
- Called from (representative examples):
  - [regexp_matches](../r/regexp_matches.md)
  - [regexp_match](../r/regexp_match.md)
  - [regexp_count](../r/regexp_count.md)
  - [regexp_instr](../r/regexp_instr.md)
  - [regexp_substr](../r/regexp_substr.md)
  - [regexp_split_to_table](../r/regexp_split_to_table.md)
  - [regexp_split_to_array](../r/regexp_split_to_array.md)

## Notes and Other Information
- Located in src/backend/utils/adt/regexp.c at lines 1442-1645
- Returns a dynamically allocated regexp_matches_ctx structure containing all match results
- Uses exponential array growth (2^n-1 pattern) to efficiently handle varying numbers of matches
- Includes protection against excessive memory usage with MaxAllocSize checking
- Handles both single-byte and multi-byte character encodings efficiently
- For multi-byte encodings, maintains both wide character and original byte representations
- The function is static (internal to the regexp.c module)

## Simplified Source

```c
static regexp_matches_ctx *setup_regexp_matches(text *orig_str, text *pattern,
                                               pg_re_flags *re_flags,
                                               int start_search, Oid collation,
                                               bool use_subpatterns,
                                               bool ignore_degenerate,
                                               bool fetching_unmatched) {
    regexp_matches_ctx *matchctx = palloc0(sizeof(regexp_matches_ctx));
    int orig_len = VARSIZE_ANY_EXHDR(orig_str);

    // Store original string for result extraction
    matchctx->orig_str = orig_str;

    // Convert string to wide character format for matching
    pg_wchar *wide_str = palloc(sizeof(pg_wchar) * (orig_len + 1));
    int wide_len = pg_mb2wchar_with_len(VARDATA_ANY(orig_str), wide_str, orig_len);

    // Compile regex pattern with appropriate flags
    int cflags = re_flags->cflags;
    if (!use_subpatterns) cflags |= REG_NOSUB;
    regex_t *cpattern = RE_compile_and_cache(pattern, cflags, collation);

    // Setup pattern counting and match arrays
    int pmatch_len = use_subpatterns && cpattern->re_nsub > 0 ?
                     cpattern->re_nsub + 1 : 1;
    matchctx->npatterns = use_subpatterns && cpattern->re_nsub > 0 ?
                          cpattern->re_nsub : 1;

    regmatch_t *pmatch = palloc(sizeof(regmatch_t) * pmatch_len);

    // Initialize dynamic result array
    int array_len = re_flags->glob ? 255 : 31;
    matchctx->match_locs = palloc(sizeof(int) * array_len);
    int array_idx = 0;
    int maxlen = 0;

    // Execute pattern matching loop
    int prev_match_end = 0;
    int prev_valid_match_end = 0;

    while (RE_wchar_execute(cpattern, wide_str, wide_len, start_search,
                           pmatch_len, pmatch)) {

        // Skip degenerate matches if requested
        if (ignore_degenerate &&
            !(pmatch[0].rm_so < wide_len && pmatch[0].rm_eo > prev_match_end))
            continue;

        // Grow result array if needed
        while (array_idx + matchctx->npatterns * 2 + 1 > array_len) {
            array_len += array_len + 1;
            matchctx->match_locs = repalloc(matchctx->match_locs,
                                          sizeof(int) * array_len);
        }

        // Store match locations
        if (use_subpatterns) {
            for (int i = 1; i <= matchctx->npatterns; i++) {
                int so = pmatch[i].rm_so, eo = pmatch[i].rm_eo;
                matchctx->match_locs[array_idx++] = so;
                matchctx->match_locs[array_idx++] = eo;
                if (so >= 0 && eo >= 0 && (eo - so) > maxlen)
                    maxlen = (eo - so);
            }
        } else {
            int so = pmatch[0].rm_so, eo = pmatch[0].rm_eo;
            matchctx->match_locs[array_idx++] = so;
            matchctx->match_locs[array_idx++] = eo;
            if (so >= 0 && eo >= 0 && (eo - so) > maxlen)
                maxlen = (eo - so);
        }

        matchctx->nmatches++;
        prev_valid_match_end = pmatch[0].rm_eo;
        prev_match_end = pmatch[0].rm_eo;

        if (!re_flags->glob) break; // Stop if not global matching

        // Advance search position
        start_search = prev_match_end;
        if (pmatch[0].rm_so == pmatch[0].rm_eo) start_search++;
        if (start_search > wide_len) break;
    }

    // Store end position for splitting operations
    matchctx->match_locs[array_idx] = wide_len;

    // Setup conversion buffer for multi-byte encodings
    int eml = pg_database_encoding_max_length();
    if (eml > 1) {
        int conv_bufsiz = (maxlen * eml > orig_len) ? orig_len + 1 : maxlen * eml + 1;
        matchctx->conv_buf = palloc(conv_bufsiz);
        matchctx->conv_bufsiz = conv_bufsiz;
        matchctx->wide_str = wide_str;
    } else {
        pfree(wide_str);
        matchctx->wide_str = NULL;
        matchctx->conv_buf = NULL;
    }

    pfree(pmatch);
    return matchctx;
}
```