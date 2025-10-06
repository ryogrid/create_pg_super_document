# build_regexp_match_result

## Location
[src/backend/utils/adt/regexp.c:1646-1701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1646-L1701)

## Overview  
Constructs a PostgreSQL array containing the captured substrings from the current regex match, handling character encoding conversion and null values for unmatched groups.

## Definition
```c
static ArrayType *build_regexp_match_result(regexp_matches_ctx *matchctx)
```

## Detailed Description
This function builds the result array for a single regex match by extracting the captured substrings from the original input text. It handles the conversion from wide character positions (used internally for Unicode-safe matching) back to the original string encoding. The function:

1. Iterates through all captured groups (subpatterns) for the current match
2. Extracts start/end positions from the match location array  
3. Handles unmatched groups by setting them to NULL
4. Performs character encoding conversion when necessary (multi-byte encodings)
5. Uses efficient substring extraction for single-byte encodings
6. Constructs a PostgreSQL array with proper dimensions and type information

The function supports both single-byte and multi-byte character encodings, choosing the most efficient extraction method based on the encoding type.

## Parameters / Member Variables
- `matchctx`: The regexp matching context containing match locations, original string, conversion buffers, and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [pg_wchar2mb_with_len](../p/pg_wchar2mb_with_len.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)  
  - [text_substr](../t/text_substr.md) (via DirectFunctionCall3)
  - [construct_md_array](../c/construct_md_array.md)
- Called from (representative examples):
  - [regexp_match](../r/regexp_match.md)
  - [regexp_matches](../r/regexp_matches.md)

## Notes and Other Information
- Located in src/backend/utils/adt/regexp.c at lines 1646-1701
- Returns an ArrayType containing text values for each captured group
- Uses pre-allocated workspace arrays (elems, nulls) from the match context for efficiency
- For multi-byte encodings, uses conversion buffer to transform wide characters back to original encoding
- For single-byte encodings, uses direct text_substr calls for better performance
- Hardcodes assumptions about text type alignment and storage (TEXTOID, TYPALIGN_INT)
- The function is static (internal to the regexp.c module)
- Array dimensions are set to match the number of capturing groups in the pattern

## Simplified Source

```c
static ArrayType *build_regexp_match_result(regexp_matches_ctx *matchctx) {
    char *buf = matchctx->conv_buf;
    Datum *elems = matchctx->elems;
    bool *nulls = matchctx->nulls;

    // Extract matching substrings from original string
    int loc = matchctx->next_match * matchctx->npatterns * 2;

    for (int i = 0; i < matchctx->npatterns; i++) {
        int so = matchctx->match_locs[loc++];  // start offset
        int eo = matchctx->match_locs[loc++];  // end offset

        if (so < 0 || eo < 0) {
            // Unmatched group - set to NULL
            elems[i] = (Datum) 0;
            nulls[i] = true;
        } else if (buf) {
            // Multi-byte encoding: convert from wide chars
            int len = pg_wchar2mb_with_len(matchctx->wide_str + so, buf, eo - so);
            elems[i] = PointerGetDatum(cstring_to_text_with_len(buf, len));
            nulls[i] = false;
        } else {
            // Single-byte encoding: direct substring extraction
            elems[i] = DirectFunctionCall3(text_substr,
                                         PointerGetDatum(matchctx->orig_str),
                                         Int32GetDatum(so + 1),
                                         Int32GetDatum(eo - so));
            nulls[i] = false;
        }
    }

    // Construct and return result array
    int dims[1] = {matchctx->npatterns};
    int lbs[1] = {1};

    return construct_md_array(elems, nulls, 1, dims, lbs,
                             TEXTOID, -1, false, TYPALIGN_INT);
}
```