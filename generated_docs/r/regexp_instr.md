# regexp_instr

## Location
[src/backend/utils/adt/regexp.c:1152-1244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1152-L1244)

## Overview
Returns the position (1-based index) of a regular expression match within a string, with support for multiple optional parameters including match occurrence, start position, and subexpression selection.

## Definition
```c
Datum regexp_instr(PG_FUNCTION_ARGS)
```

## Detailed Description
The regexp_instr function is a comprehensive regular expression position-finding function that locates pattern matches within text strings. It supports various parameters to control matching behavior:

- Finds the nth occurrence of a pattern match
- Allows specification of starting position within the string
- Can return either start or end position of matches (controlled by endoption)
- Supports subexpression matching for capturing groups
- Validates all input parameters and provides detailed error messages for invalid values

The function uses PostgreSQL's internal regular expression engine and integrates with the collation system for locale-aware matching. It returns 0 when no match is found or when the requested occurrence/subexpression doesn't exist.

## Parameters / Member Variables
- `str`: Input text string to search within (argument 0)
- `pattern`: Regular expression pattern to match (argument 1) 
- `start`: Starting position for search (1-based, argument 2, default: 1)
- `n`: Which occurrence of the match to return (argument 3, default: 1)
- `endoption`: Whether to return start (0) or end (1) position (argument 4, default: 0)
- `flags`: Regular expression flags for matching behavior (argument 5, optional)
- `subexpr`: Which subexpression/capture group to return position for (argument 6, default: 0 for whole match)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP_IF_EXISTS: Extract optional text arguments
  - [parse_re_flags](../p/parse_re_flags.md): Parse regular expression flags
  - [setup_regexp_matches](../s/setup_regexp_matches.md): Set up pattern matching context
  - PG_GET_COLLATION: Get current collation for locale-aware matching
  - PG_NARGS: Get number of function arguments
  - Various PostgreSQL error reporting functions

- Called from (representative examples):
  - [regexp_instr_no_start](regexp_instr_no_start.md): Wrapper without start parameter
  - [regexp_instr_no_n](regexp_instr_no_n.md): Wrapper without occurrence parameter
  - [regexp_instr_no_endoption](regexp_instr_no_endoption.md): Wrapper without end option parameter
  - [regexp_instr_no_flags](regexp_instr_no_flags.md): Wrapper without flags parameter
  - [regexp_instr_no_subexpr](regexp_instr_no_subexpr.md): Wrapper without subexpression parameter

## Notes and Other Information
- Returns 1-based positions (PostgreSQL SQL standard)
- Returns 0 when no match is found or parameters are out of range
- Does not support the global 'g' flag (generates error if specified)
- Internally enables global matching to find all occurrences up to the nth match
- Validates that start > 0, n > 0, endoption ∈ {0,1}, and subexpr ≥ 0
- Part of PostgreSQL's SQL standard regular expression function suite
- Used by several wrapper functions that provide different parameter combinations

## Simplified Source

```c
Datum
regexp_instr(PG_FUNCTION_ARGS)
{
    text *str = PG_GETARG_TEXT_PP(0);
    text *pattern = PG_GETARG_TEXT_PP(1);
    int start = 1;
    int n = 1;
    int endoption = 0;
    text *flags = PG_GETARG_TEXT_PP_IF_EXISTS(5);
    int subexpr = 0;
    int pos;
    pg_re_flags re_flags;
    regexp_matches_ctx *matchctx;

    // Parse optional parameters with validation
    if (PG_NARGS() > 2) {
        start = PG_GETARG_INT32(2);
        if (start <= 0) ereport(ERROR, "invalid start position");
    }
    if (PG_NARGS() > 3) {
        n = PG_GETARG_INT32(3);
        if (n <= 0) ereport(ERROR, "invalid occurrence number");
    }
    if (PG_NARGS() > 4) {
        endoption = PG_GETARG_INT32(4);
        if (endoption != 0 && endoption != 1) ereport(ERROR, "invalid endoption");
    }
    if (PG_NARGS() > 6) {
        subexpr = PG_GETARG_INT32(6);
        if (subexpr < 0) ereport(ERROR, "invalid subexpression number");
    }

    // Setup regex flags (reject global flag, but enable it internally)
    parse_re_flags(&re_flags, flags);
    if (re_flags.glob) ereport(ERROR, "global option not supported");
    re_flags.glob = true;  // Enable internally to find all matches

    // Perform pattern matching
    matchctx = setup_regexp_matches(str, pattern, &re_flags, start - 1,
                                   PG_GET_COLLATION(), (subexpr > 0), false, false);

    // Check if requested match/subexpression exists
    if (n > matchctx->nmatches || subexpr > matchctx->npatterns) {
        PG_RETURN_INT32(0);
    }

    // Calculate position index in match array
    pos = (n - 1) * matchctx->npatterns;
    if (subexpr > 0) pos += subexpr - 1;
    pos *= 2;
    if (endoption == 1) pos += 1;  // Return end position instead of start

    // Return 1-based position or 0 if not found
    if (matchctx->match_locs[pos] >= 0) {
        PG_RETURN_INT32(matchctx->match_locs[pos] + 1);
    } else {
        PG_RETURN_INT32(0);
    }
}
```