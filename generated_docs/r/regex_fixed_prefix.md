# regex_fixed_prefix

## Location
[src/backend/utils/adt/like_support.c:1099-1166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L1099-L1166)

## Overview
Extracts the fixed prefix portion from a regular expression pattern to support query optimization by identifying literal characters at the beginning of the regex.

## Definition

```c
static Pattern_Prefix_Status
regex_fixed_prefix(Const *patt_const, bool case_insensitive, Oid collation,
				   Const **prefix_const, Selectivity *rest_selec)
```
## Detailed Description
This function analyzes regular expression patterns to extract the fixed (literal) prefix portion that appears before any regex metacharacters or variable components. This analysis enables PostgreSQL's query optimizer to use index scans when regex patterns start with literal characters, significantly improving query performance for patterns like '^literal_text.*'.

The function delegates the actual prefix extraction to the `regexp_fixed_prefix` function from the regex engine, which handles the complex task of parsing regex syntax to identify the literal prefix. It also calculates selectivity estimates for the remaining portion of the pattern after the prefix.

## Parameters / Member Variables
- `patt_const`: Input Const node containing the regular expression pattern (must be TEXT type)
- `case_insensitive`: Boolean indicating whether matching should be case-insensitive
- `collation`: OID of the collation to use for case-insensitive operations
- `prefix_const`: Output parameter set to a Const node containing the extracted prefix, or NULL if no prefix exists
- `rest_selec`: Output parameter set to selectivity estimate for the remainder of the pattern after the prefix

## Dependencies
- Functions called/Symbols referenced:
  - [regexp_fixed_prefix](regexp_fixed_prefix.md): Core regex engine function that extracts literal prefix from regex pattern
  - `DatumGetTextPP`/`TextDatumGetCString`: Extract pattern string from Const node
  - [regex_selectivity](regex_selectivity.md): Estimate selectivity of regex pattern or remaining portion
  - [string_to_const](../s/string_to_const.md): Create Const node for extracted prefix
- Called from (representative examples):
  - [pattern_fixed_prefix](../p/pattern_fixed_prefix.md): Generic pattern prefix extraction function

## Notes and Other Information
- This is a static function located in `src/backend/utils/adt/like_support.c:1099-1166`
- Only supports TEXT data type; explicitly rejects BYTEA with an error message
- The function relies on the regex engine's `regexp_fixed_prefix` for the actual analysis
- Returns `Pattern_Prefix_Exact` if the regex matches exactly one string (no variable components)
- Returns `Pattern_Prefix_Partial` if a non-empty literal prefix exists before variable regex components
- Returns `Pattern_Prefix_None` if no fixed prefix can be extracted
- For exact matches, the rest selectivity is set to 1.0 (100% selectivity)
- The function is conservative in its analysis to ensure correctness of query results
- Used by the query planner to optimize regex operations like SIMILAR TO and ~ operators

## Simplified Source

```c
// Simplified version of regex_fixed_prefix
static Pattern_Prefix_Status regex_fixed_prefix(Const *patt_const, bool case_insensitive,
                                               Oid collation, Const **prefix_const,
                                               Selectivity *rest_selec) {
    // Only TEXT type supported, reject BYTEA
    if (patt_const->consttype == BYTEAOID)
        ereport(ERROR, (errmsg("regex not supported on bytea")));

    // Extract fixed prefix from regex pattern
    char *prefix = regexp_fixed_prefix(DatumGetTextPP(patt_const->constvalue),
                                     case_insensitive, collation, &exact);

    if (prefix == NULL) {
        // No prefix found - calculate selectivity for entire pattern
        *prefix_const = NULL;
        if (rest_selec != NULL) {
            char *patt = TextDatumGetCString(patt_const->constvalue);
            *rest_selec = regex_selectivity(patt, strlen(patt), case_insensitive, 0);
            pfree(patt);
        }
        return Pattern_Prefix_None;
    }

    // Found prefix - create const and calculate remaining selectivity
    *prefix_const = string_to_const(prefix, patt_const->consttype);

    if (rest_selec != NULL) {
        if (exact) {
            *rest_selec = 1.0;  // Exact match
        } else {
            char *patt = TextDatumGetCString(patt_const->constvalue);
            *rest_selec = regex_selectivity(patt, strlen(patt), case_insensitive, strlen(prefix));
            pfree(patt);
        }
    }

    pfree(prefix);
    return exact ? Pattern_Prefix_Exact : Pattern_Prefix_Partial;
}
```