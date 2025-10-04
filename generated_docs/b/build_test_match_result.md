# build_test_match_result

## Location
[src/test/modules/test_regex/test_regex.c:692-763](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_regex/test_regex.c#L692-L763)

## Overview
A static helper function in PostgreSQL's test_regex module that builds an output array containing regex match results or indices for testing purposes.

## Definition

```c
struct_md_array(elems, nulls, 1, dims, lbs,
							  TEXTOID, -1, false, TYPALIGN_INT);
```
## Detailed Description
The  function constructs a PostgreSQL array containing the results of a regular expression match operation. The function operates in two modes based on the  flag in the match context:

1. **String Mode**: Returns the actual matched substrings extracted from the original input string
2. **Indices Mode**: Returns the start/end positions of matches as formatted strings ("start end")

The function processes all captured groups (patterns) in the regex match and handles special cases such as non-matching groups (represented as NULL values) and the REG_EXPECT flag which provides additional match details.

For string extraction, the function supports both wide character conversion (using a conversion buffer) and direct text substring operations, depending on the available resources in the match context.

## Parameters / Member Variables
- : Pointer to test_regex_ctx structure containing:
  - : Conversion buffer for wide character to multibyte conversion
  - : Array to store result datums
  - : Array to track NULL values in results
  - : Boolean flag indicating whether to return indices or strings
  - : Index of current match being processed
  - : Number of capturing groups in the regex
  - : Array containing start/end positions of matches
  - : Wide character representation of the original string
  - : Original input string
  - : Additional match information for REG_EXPECT mode

## Dependencies
- Functions called/Symbols referenced:
  - [cstring_to_text](../c/cstring_to_text.md)
  - [pg_wchar2mb_with_len](../p/pg_wchar2mb_with_len.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
  - [text_substr](../t/text_substr.md)
  - DirectFunctionCall3
  - [construct_md_array](../c/construct_md_array.md)
  - REG_EXPECT
  - TYPALIGN_INT
- Called from (representative examples):
  - [test_regex](../t/test_regex.md)

## Notes and Other Information
- This is a static function used exclusively within the test_regex module for testing regex functionality
- The function handles both successful matches and non-matches (represented as NULL array elements)
- In indices mode, end positions are reported as "eo - 1" for consistency with Tcl regex behavior
- When REG_EXPECT flag is set in indices mode, additional "details" information is appended to the result array
- The function constructs a one-dimensional PostgreSQL array with text elements using hardcoded assumptions about the text type
- Memory management relies on the caller's memory context for array construction

## Simplified Source

```c
// Simplified version of build_test_match_result
static ArrayType *
build_test_match_result(test_regex_ctx *matchctx)
{
    char *buf = matchctx->conv_buf;
    Datum *elems = matchctx->elems;
    bool *nulls = matchctx->nulls;
    bool indices = matchctx->re_flags.indices;
    char bufstr[80];
    int loc = matchctx->next_match * matchctx->npatterns * 2;

    // Extract matching substrings or indices from original string
    for (int i = 0; i < matchctx->npatterns; i++) {
        int so = matchctx->match_locs[loc++];  // start offset
        int eo = matchctx->match_locs[loc++];  // end offset

        if (indices) {
            // Return indices as "start end" string
            snprintf(bufstr, sizeof(bufstr), "%d %d",
                     so, so < 0 ? eo : eo - 1);
            elems[i] = PointerGetDatum(cstring_to_text(bufstr));
            nulls[i] = false;
        } else if (so < 0 || eo < 0) {
            // No match for this pattern
            elems[i] = (Datum) 0;
            nulls[i] = true;
        } else {
            // Extract actual substring
            if (buf) {
                // Convert wide chars to multibyte
                int len = pg_wchar2mb_with_len(matchctx->wide_str + so, buf, eo - so);
                elems[i] = PointerGetDatum(cstring_to_text_with_len(buf, len));
            } else {
                // Direct substring extraction
                elems[i] = DirectFunctionCall3(text_substr,
                                               PointerGetDatum(matchctx->orig_str),
                                               Int32GetDatum(so + 1),
                                               Int32GetDatum(eo - so));
            }
            nulls[i] = false;
        }
    }

    // Add details for EXPECT mode if needed
    if (indices && (matchctx->re_flags.cflags & REG_EXPECT)) {
        int so = matchctx->details.rm_extend.rm_so;
        int eo = matchctx->details.rm_extend.rm_eo;
        snprintf(bufstr, sizeof(bufstr), "%d %d",
                 so, so < 0 ? eo : eo - 1);
        elems[i] = PointerGetDatum(cstring_to_text(bufstr));
        nulls[i] = false;
        i++;
    }

    // Create and return PostgreSQL array
    int dims[1] = {i};
    int lbs[1] = {1};
    return construct_md_array(elems, nulls, 1, dims, lbs,
                              TEXTOID, -1, false, TYPALIGN_INT);
}
```