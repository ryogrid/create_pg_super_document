# test_regex_ctx

## Location
src/test/modules/test_regex/test_regex.c: 38 - 55

## Overview
A cross-call state structure that maintains context and intermediate results during regex testing operations in PostgreSQL's test_regex module.

## Definition
```c
typedef struct test_regex_ctx
{
    test_re_flags re_flags;     /* flags */
    rm_detail_t details;        /* "details" from execution */
    text       *orig_str;       /* data string in original TEXT form */
    int         nmatches;       /* number of places where pattern matched */
    int         npatterns;      /* number of capturing subpatterns */
    /* We store start char index and end+1 char index for each match */
    /* so the number of entries in match_locs is nmatches * npatterns * 2 */
    int        *match_locs;     /* 0-based character indexes */
    int         next_match;     /* 0-based index of next match to process */
    /* workspace for build_test_match_result() */
    Datum      *elems;          /* has npatterns+1 elements */
    bool       *nulls;          /* has npatterns+1 elements */
    pg_wchar   *wide_str;       /* wide-char version of original string */
    char       *conv_buf;       /* conversion buffer, if needed */
    int         conv_bufsiz;    /* size thereof */
} test_regex_ctx;
```

## Detailed Description
The test_regex_ctx structure serves as a comprehensive context holder for regex testing operations. It maintains state across multiple function calls, storing both input parameters and intermediate results. This structure is essential for managing complex regex operations that may involve multiple matches, character encoding conversions, and result formatting.

## Parameters / Member Variables
- `re_flags`: Configuration flags inherited from test_re_flags structure
- `details`: Detailed execution information from Spencer's regex engine
- `orig_str`: Original input string in PostgreSQL's TEXT format
- `nmatches`: Count of successful pattern matches found
- `npatterns`: Number of capturing groups (subpatterns) in the regex
- `match_locs`: Array storing start and end+1 character indices for each match
- `next_match`: Index tracking the next match to be processed
- `elems`: Workspace array for building result tuples (size: npatterns+1)
- `nulls`: Boolean array indicating null values in result tuples
- `wide_str`: Wide-character representation of the original string for Unicode support
- `conv_buf`: Buffer for character encoding conversions when needed
- `conv_bufsiz`: Size of the conversion buffer

## Dependencies
- Functions called/Symbols referenced:
  - test_re_flags (embedded structure)
  - rm_detail_t (Spencer's regex detail type)
- Used by:
  - test_regex (primary function)
  - parse_test_flags (for initialization)
  - setup_test_matches (for match processing)
  - build_test_match_result (for result building)

## Notes and Other Information
This structure is designed to efficiently handle complex regex operations involving multiple matches and various output formats. The match_locs array uses a specific layout where each match stores start and end+1 indices, allowing precise character-level result reporting. The workspace arrays (elems, nulls) facilitate building PostgreSQL tuple results without repeated memory allocation.