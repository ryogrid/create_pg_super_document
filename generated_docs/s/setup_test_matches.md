# setup_test_matches

## Location
[src/test/modules/test_regex/test_regex.c:435-617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_regex/test_regex.c#L435-L617)

## Overview
setup_test_matches is a static function that performs comprehensive regex matching on input text, executing the pattern potentially multiple times and storing all match results in a structured context for later retrieval.

## Definition
static test_regex_ctx *setup_test_matches(text *orig_str, regex_t *cpattern, test_re_flags *re_flags, Oid collation, bool use_subpatterns)

## Detailed Description
This function performs the core regex matching operation by executing a compiled pattern against input text and storing all match results. It handles both single and global matching modes, manages memory efficiently through dynamic allocation, and supports both full pattern matches and subpattern captures.

Key operations include:
1. Converting input text from database encoding to wide characters for regex engine
2. Setting up output storage with dynamic growth for match results  
3. Executing regex pattern repeatedly (if glob flag set) until no more matches
4. Handling zero-length matches by advancing search position
5. Managing subpattern capture when requested
6. Special handling for partial matches when no full matches found
7. Optimizing memory usage for single-byte vs multibyte character sets

The function returns a test_regex_ctx structure containing all match information for subsequent result building.

## Parameters / Member Variables
- : TEXT object containing the input string to search
- : Compiled regex_t pattern to execute  
- : test_re_flags structure controlling matching behavior
- : OID of collation for character classification (currently unused)
- : Boolean indicating whether to capture subpattern matches

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)/palloc (PostgreSQL memory allocation)
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md) (gets max character length)
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md) (converts multibyte to wide characters)
  - [test_re_execute](../t/test_re_execute.md) (executes regex pattern)
  - [repalloc](../r/repalloc.md) (reallocates memory with larger size)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - MaxAllocSize (PostgreSQL memory limit constant)
  - ereport/ERROR (PostgreSQL error reporting)
- Called from (representative examples):
  - [test_regex](../t/test_regex.md) (main regex testing function)

## Notes and Other Information
- This is a static (internal) function within the test_regex module
- Uses dynamic memory allocation with exponential growth (2^n-1 pattern)
- Handles global matching by repeatedly executing until no more matches found
- Correctly advances past zero-length matches to avoid infinite loops
- Optimizes memory usage by keeping wide string only for multibyte encodings
- Supports partial matching details when no full matches are found
- Stores match locations as integer pairs (start, end) for each subpattern
- Located in src/test/modules/test_regex/test_regex.c:435-617

## Simplified Source

```c
static test_regex_ctx *setup_test_matches(text *orig_str, regex_t *cpattern,
                                         test_re_flags *re_flags, Oid collation,
                                         bool use_subpatterns) {
    test_regex_ctx *matchctx = palloc0(sizeof(test_regex_ctx));
    int eml = pg_database_encoding_max_length();
    int orig_len;
    pg_wchar *wide_str;
    int wide_len;
    regmatch_t *pmatch;
    int pmatch_len;
    int array_len;
    int array_idx;
    int start_search;
    int maxlen = 0;

    // Save flags and original string
    matchctx->re_flags = *re_flags;
    matchctx->orig_str = orig_str;

    // Convert string to wide characters for regex engine
    orig_len = VARSIZE_ANY_EXHDR(orig_str);
    wide_str = (pg_wchar *) palloc(sizeof(pg_wchar) * (orig_len + 1));
    wide_len = pg_mb2wchar_with_len(VARDATA_ANY(orig_str), wide_str, orig_len);

    // Determine number of patterns to capture
    if (use_subpatterns && cpattern->re_nsub > 0) {
        matchctx->npatterns = cpattern->re_nsub + 1;
        pmatch_len = cpattern->re_nsub + 1;
    } else {
        use_subpatterns = false;
        matchctx->npatterns = 1;
        pmatch_len = 1;
    }

    // Allocate temporary match array and dynamic result storage
    pmatch = palloc(sizeof(regmatch_t) * pmatch_len);
    array_len = re_flags->glob ? 255 : 31;  // Start size: 2^n-1
    matchctx->match_locs = (int *) palloc(sizeof(int) * array_len);
    array_idx = 0;

    // Execute pattern repeatedly until no more matches
    start_search = 0;
    while (test_re_execute(cpattern, wide_str, wide_len, start_search,
                          &matchctx->details, pmatch_len, pmatch, re_flags->eflags)) {

        // Grow storage if needed
        while (array_idx + matchctx->npatterns * 2 + 1 > array_len) {
            array_len += array_len + 1;  // 2^n-1 => 2^(n+1)-1
            if (array_len > MaxAllocSize / sizeof(int))
                ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                               errmsg("too many regular expression matches")));
            matchctx->match_locs = (int *) repalloc(matchctx->match_locs,
                                                   sizeof(int) * array_len);
        }

        // Store match locations for all patterns
        for (int i = 0; i < matchctx->npatterns; i++) {
            int so = pmatch[i].rm_so;
            int eo = pmatch[i].rm_eo;
            matchctx->match_locs[array_idx++] = so;
            matchctx->match_locs[array_idx++] = eo;
            if (so >= 0 && eo >= 0 && (eo - so) > maxlen)
                maxlen = (eo - so);
        }
        matchctx->nmatches++;

        // Stop if not global matching
        if (!re_flags->glob) break;

        // Advance search position, handle zero-length matches
        start_search = pmatch[0].rm_eo;
        if (pmatch[0].rm_so == pmatch[0].rm_eo)
            start_search++;
        if (start_search > wide_len) break;
    }

    // Handle partial match details when no matches found
    if (matchctx->nmatches == 0 && re_flags->partial && re_flags->indices) {
        // Ensure space and store partial match details
        while (array_idx + matchctx->npatterns * 2 + 1 > array_len) {
            array_len += array_len + 1;
            matchctx->match_locs = (int *) repalloc(matchctx->match_locs,
                                                   sizeof(int) * array_len);
        }
        matchctx->match_locs[array_idx++] = matchctx->details.rm_extend.rm_so;
        matchctx->match_locs[array_idx++] = matchctx->details.rm_extend.rm_eo;
        for (int i = 1; i < matchctx->npatterns; i++) {
            matchctx->match_locs[array_idx++] = -1;
            matchctx->match_locs[array_idx++] = -1;
        }
        matchctx->nmatches++;
    }

    // Setup conversion buffer for multibyte encodings
    if (eml > 1) {
        int64 maxsiz = eml * (int64) maxlen;
        int conv_bufsiz = (maxsiz > orig_len) ? orig_len + 1 : maxsiz + 1;
        matchctx->conv_buf = palloc(conv_bufsiz);
        matchctx->conv_bufsiz = conv_bufsiz;
        matchctx->wide_str = wide_str;
    } else {
        // Single-byte encoding - don't need wide string
        pfree(wide_str);
        matchctx->wide_str = NULL;
        matchctx->conv_buf = NULL;
        matchctx->conv_bufsiz = 0;
    }

    pfree(pmatch);
    return matchctx;
}
```