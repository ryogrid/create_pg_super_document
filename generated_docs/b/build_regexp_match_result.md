# build_regexp_match_result

## Location
src/backend/utils/adt/regexp.c: 1646 - 1701

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
  - pg_wchar2mb_with_len
  - cstring_to_text_with_len  
  - text_substr (via DirectFunctionCall3)
  - construct_md_array
- Called from (representative examples):
  - regexp_match
  - regexp_matches

## Notes and Other Information
- Located in src/backend/utils/adt/regexp.c at lines 1646-1701
- Returns an ArrayType containing text values for each captured group
- Uses pre-allocated workspace arrays (elems, nulls) from the match context for efficiency
- For multi-byte encodings, uses conversion buffer to transform wide characters back to original encoding
- For single-byte encodings, uses direct text_substr calls for better performance
- Hardcodes assumptions about text type alignment and storage (TEXTOID, TYPALIGN_INT)
- The function is static (internal to the regexp.c module)
- Array dimensions are set to match the number of capturing groups in the pattern