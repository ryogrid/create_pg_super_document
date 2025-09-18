# regexp_match

## Location
[src/backend/utils/adt/regexp.c:1321-1356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1321-L1356)

## Overview
Returns the first substring(s) matching a regular expression pattern within a string, including any captured subgroups from parenthesized subexpressions.

## Definition
```c
Datum regexp_match(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regexp_match` function extracts the first match of a regular expression pattern from within an input string. Unlike `regexp_like` which only returns boolean results, this function returns the actual matched text as an array. If the pattern contains parenthesized subexpressions (capture groups), the function returns an array containing the overall match plus all captured subgroups.

The function performs comprehensive pattern matching using PostgreSQL's advanced regular expression engine. It sets up a complete matching context that can handle subpatterns and provides detailed match location information. If no match is found, the function returns NULL.

Key behavioral characteristics:
- Returns only the first match (prohibits 'g' flag)
- Supports capture groups and subpattern matching
- Returns an array of text values for matches and submatches
- Uses efficient wide-character internal processing

## Parameters / Member Variables
- `orig_str`: The input text string to be searched (PG_GETARG_TEXT_PP(0))
- `pattern`: The regular expression pattern to match against (PG_GETARG_TEXT_PP(1))  
- `flags`: Optional text parameter containing regex flags (PG_GETARG_TEXT_PP_IF_EXISTS(2))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP_IF_EXISTS
  - [pg_re_flags](../p/pg_re_flags.md) (struct type)
  - [regexp_matches_ctx](regexp_matches_ctx.md) (struct type)
  - [parse_re_flags](../p/parse_re_flags.md)
  - [setup_regexp_matches](../s/setup_regexp_matches.md)
  - PG_GET_COLLATION
  - [build_regexp_match_result](../b/build_regexp_match_result.md)
  - PG_RETURN_DATUM
- Called from (representative examples):
  - [regexp_match_no_flags](regexp_match_no_flags.md)

## Notes and Other Information
- Returns text array containing matched strings and captured subgroups
- Prohibits the global ('g') flag - suggests using regexp_matches() for global matching
- Allocates workspace for building result arrays (elems and nulls arrays)
- Uses setup_regexp_matches() with subpattern support enabled (use_subpatterns=true)
- Returns NULL when no matches are found
- Located in src/backend/utils/adt/regexp.c:1321-1356
- Part of PostgreSQL's SQL standard regex function family
- More complex than regexp_like as it extracts actual match data rather than just testing for existence
- Handles wide-character strings internally for proper Unicode support