# regexp_fixed_prefix

## Location
[src/backend/utils/adt/regexp.c:1979-2035](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1979-L2035)

## Overview
Extracts a fixed prefix from a regular expression pattern, returning the longest literal string that must appear at the beginning of any string matching the regular expression.

## Definition

```c
char *
regexp_fixed_prefix(text *text_re, bool case_insensitive, Oid collation,
					bool *exact)
```
## Detailed Description
This function analyzes a regular expression to determine if it has a fixed prefix - a literal string that must appear at the beginning of any matching text. This optimization is crucial for PostgreSQL's query planner as it allows index scans to be used more efficiently when searching for patterns that start with literal text.

The function compiles the regular expression with appropriate flags, then uses the underlying regex library's prefix extraction capabilities () to identify any fixed prefix. If the entire pattern is a literal string (no regex metacharacters), it sets the  flag to indicate a complete match rather than just a prefix.

The extracted prefix is converted from the internal wide character representation back to the database encoding before being returned as a palloc'd string that the caller must free.

## Parameters / Member Variables
- `*text_re`: The input regular expression pattern as a PostgreSQL text datum
- `case_insensitive`: Boolean flag indicating whether the regex should be case-insensitive
- `collation`: The collation OID to use for character comparisons and case folding
- `*exact`: Output parameter set to true if the entire pattern is an exact literal match (not just a prefix)
## Dependencies
- Functions called/Symbols referenced:
  - [RE_compile_and_cache](../R/RE_compile_and_cache.md)
  - [pg_regprefix](../p/pg_regprefix.md)
  - [pg_regerror](../p/pg_regerror.md)
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md)
  - [pg_wchar2mb_with_len](../p/pg_wchar2mb_with_len.md)
  - regex_t (type)
  - REG_ADVANCED, REG_ICASE, REG_NOSUB (constants)
  - REG_NOMATCH, REG_PREFIX, REG_EXACT (result codes)
- Called from (representative examples):
  - [regex_fixed_prefix](regex_fixed_prefix.md) (in src/backend/utils/adt/like_support.c)

## Notes and Other Information
- Returns NULL if no fixed prefix can be extracted from the pattern
- The returned string is palloc'd and must be freed by the caller
- Used primarily by the query optimizer to enable index scans on regex patterns with literal prefixes
- Handles both case-sensitive and case-insensitive matching through compilation flags
- Supports PostgreSQL's advanced regex features through the REG_ADVANCED flag
- Error handling includes proper reporting of regex compilation failures with descriptive messages

## Simplified Source

```c
char *
regexp_fixed_prefix(text *text_re, bool case_insensitive, Oid collation, bool *exact)
{
    char *result;
    regex_t *re;
    int cflags;
    int re_result;
    pg_wchar *str;
    size_t slen;
    size_t maxlen;

    *exact = false;  // Default: not an exact match

    // Set up compilation flags
    cflags = REG_ADVANCED;
    if (case_insensitive)
        cflags |= REG_ICASE;

    // Compile the regular expression
    re = RE_compile_and_cache(text_re, cflags | REG_NOSUB, collation);

    // Extract the fixed prefix
    re_result = pg_regprefix(re, &str, &slen);

    switch (re_result) {
        case REG_NOMATCH:
            return NULL;  // No fixed prefix found

        case REG_PREFIX:
            // Found a prefix, continue with conversion
            break;

        case REG_EXACT:
            *exact = true;  // Entire pattern is literal
            break;

        default:
            // Report regex compilation error
            ereport(ERROR, (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
                           errmsg("regular expression failed")));
            break;
    }

    // Convert wide chars back to database encoding
    maxlen = pg_database_encoding_max_length() * slen + 1;
    result = (char *) palloc(maxlen);
    slen = pg_wchar2mb_with_len(str, result, slen);

    pfree(str);
    return result;
}
```