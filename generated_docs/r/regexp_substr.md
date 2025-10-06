# regexp_substr

## Location
[src/backend/utils/adt/regexp.c:1858-1945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1858-L1945)

## Overview
Returns the substring that matches a regular expression pattern, providing full parameter control for pattern matching position, occurrence number, flags, and subexpression selection.

## Definition

```c
Datum
regexp_substr(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the main implementation for PostgreSQL's REGEXP_SUBSTR SQL function. It extracts and returns a substring from an input text that matches a specified regular expression pattern. The function supports up to 6 parameters:

1. Input text string
2. Regular expression pattern
3. Start position (optional, defaults to 1)
4. Occurrence number (optional, defaults to 1 - first match)
5. Flags for regex behavior (optional)
6. Subexpression number (optional, defaults to 0 - full match)

The function performs comprehensive parameter validation, sets up the regex matching context using , and extracts the appropriate substring based on the specified occurrence and subexpression. It returns NULL if no match is found, if the requested occurrence exceeds available matches, or if the requested subexpression doesn't exist.

## Parameters / Member Variables
-  (text): Input text string to search within
-  (text): Regular expression pattern to match
-  (int32): Start position in string (1-based, optional, defaults to 1)
-  (int32): Which occurrence to return (1-based, optional, defaults to 1)
-  (text): Regex flags (optional, e.g., 'i' for case-insensitive)
-  (int32): Subexpression number (0-based, optional, defaults to 0 for full match)

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from (representative examples):
  - 
  -  
  - 
  - 

## Notes and Other Information
- The function prohibits the 'g' (global) flag from being specified by users, but internally enables it to find all matches
- Extensive parameter validation ensures start > 0, n > 0, and subexpr >= 0
- Returns NULL for invalid match positions, missing occurrences, or non-existent subexpressions
- The function is located in src/backend/utils/adt/regexp.c:1858-1945
- Other regexp_substr variants delegate to this main function with default parameter values

## Simplified Source

```c
Datum regexp_substr(PG_FUNCTION_ARGS) {
    text *str = PG_GETARG_TEXT_PP(0);
    text *pattern = PG_GETARG_TEXT_PP(1);
    int start = 1;
    int n = 1;
    text *flags = PG_GETARG_TEXT_PP_IF_EXISTS(4);
    int subexpr = 0;

    // Parse optional parameters with validation
    if (PG_NARGS() > 2) {
        start = PG_GETARG_INT32(2);
        if (start <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("invalid value for parameter \"start\": %d", start)));
    }

    if (PG_NARGS() > 3) {
        n = PG_GETARG_INT32(3);
        if (n <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("invalid value for parameter \"n\": %d", n)));
    }

    if (PG_NARGS() > 5) {
        subexpr = PG_GETARG_INT32(5);
        if (subexpr < 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("invalid value for parameter \"subexpr\": %d", subexpr)));
    }

    // Parse regex flags but reject global flag
    pg_re_flags re_flags;
    parse_re_flags(&re_flags, flags);

    if (re_flags.glob)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("regexp_substr() does not support the \"global\" option")));

    // Force global matching internally to find all occurrences
    re_flags.glob = true;

    // Setup regexp matching
    regexp_matches_ctx *matchctx = setup_regexp_matches(str, pattern, &re_flags,
                                                        start - 1, PG_GET_COLLATION(),
                                                        (subexpr > 0), false, false);

    // Return NULL if requested occurrence or subexpression not found
    if (n > matchctx->nmatches || subexpr > matchctx->npatterns)
        PG_RETURN_NULL();

    // Calculate position of requested match/subexpression
    int pos = (n - 1) * matchctx->npatterns;
    if (subexpr > 0) pos += subexpr - 1;
    pos *= 2;

    int so = matchctx->match_locs[pos];
    int eo = matchctx->match_locs[pos + 1];

    if (so < 0 || eo < 0)
        PG_RETURN_NULL();  // unidentifiable location

    // Extract and return the matching substring
    PG_RETURN_DATUM(DirectFunctionCall3(text_substr,
                                       PointerGetDatum(matchctx->orig_str),
                                       Int32GetDatum(so + 1),
                                       Int32GetDatum(eo - so)));
}
```