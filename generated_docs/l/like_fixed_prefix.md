# like_fixed_prefix

## Location
[src/backend/utils/adt/like_support.c:992-1098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L992-L1098)

## Overview
Extracts the fixed prefix portion from a LIKE pattern string to support query optimization by identifying non-wildcard characters at the beginning of the pattern.

## Definition

```c
static Pattern_Prefix_Status
like_fixed_prefix(Const *patt_const, bool case_insensitive, Oid collation,
				  Const **prefix_const, Selectivity *rest_selec)
```
## Detailed Description
This function analyzes LIKE patterns to extract the fixed (literal) prefix portion that appears before any wildcard characters (% or _). This analysis is crucial for PostgreSQL's query optimizer as it allows the use of index scans when patterns start with literal characters. The function handles both case-sensitive and case-insensitive matching, supports TEXT and BYTEA data types, and properly handles escaped characters.

The function processes the pattern character by character, stopping when it encounters wildcards (% or _), escape sequences, or case-varying alphabetic characters (in case-insensitive mode). It returns information about whether the pattern has no fixed prefix, a partial prefix, or represents an exact match.

## Parameters / Member Variables
- `patt_const`: Input Const node containing the LIKE pattern (TEXT or BYTEA)
- `case_insensitive`: Boolean indicating whether matching should be case-insensitive (ILIKE)
- `collation`: OID of the collation to use for case-insensitive operations
- `prefix_const`: Output parameter set to a Const node containing the extracted prefix, or NULL if no prefix exists
- `rest_selec`: Output parameter set to selectivity estimate for the remainder of the pattern after the prefix

## Dependencies
- Functions called/Symbols referenced:
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md): Check if database uses multibyte encoding
  - [lc_ctype_is_c](lc_ctype_is_c.md)/`pg_newlocale_from_collation`: Locale handling for case-insensitive operations
  - `TextDatumGetCString`/`DatumGetByteaPP`: Extract pattern string from Const node
  - [pattern_char_isalpha](../p/pattern_char_isalpha.md): Check if character is alphabetic (case-varying)
  - [string_to_const](../s/string_to_const.md)/`string_to_bytea_const`: Create Const node for extracted prefix
  - [like_selectivity](like_selectivity.md): Estimate selectivity of remaining pattern portion
- Called from (representative examples):
  - [pattern_fixed_prefix](../p/pattern_fixed_prefix.md): Generic pattern prefix extraction function

## Notes and Other Information
- This is a static function located in `src/backend/utils/adt/like_support.c:992-1098`
- The function is conservative in its analysis - it may report a shorter prefix than the true fixed prefix to avoid incorrect query results
- Handles escape sequences properly (backslash followed by any character)
- For case-insensitive patterns, stops at alphabetic characters that could vary in case
- Returns `Pattern_Prefix_Exact` if the entire pattern is literal (no wildcards)
- Returns `Pattern_Prefix_Partial` if a non-empty prefix exists before wildcards
- Returns `Pattern_Prefix_None` if no fixed prefix can be extracted
- Case-insensitive matching is not supported for BYTEA data type

## Simplified Source

```c
static Pattern_Prefix_Status
like_fixed_prefix(Const *patt_const, bool case_insensitive, Oid collation,
                  Const **prefix_const, Selectivity *rest_selec)
{
    char *match, *patt;
    int pattlen, match_pos = 0;
    Oid typeid = patt_const->consttype;
    bool is_multibyte = (pg_database_encoding_max_length() > 1);
    pg_locale_t locale = 0;
    bool locale_is_c = false;

    // Validate input type (TEXT or BYTEA)
    Assert(typeid == BYTEAOID || typeid == TEXTOID);

    // Handle case-insensitive setup
    if (case_insensitive) {
        // Error if BYTEA with case-insensitive
        if (typeid == BYTEAOID)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("case insensitive matching not supported on type bytea")));

        // Setup locale for case-insensitive matching
        if (lc_ctype_is_c(collation))
            locale_is_c = true;
        else
            locale = pg_newlocale_from_collation(collation);
    }

    // Extract pattern string
    if (typeid != BYTEAOID) {
        patt = TextDatumGetCString(patt_const->constvalue);
        pattlen = strlen(patt);
    } else {
        bytea *bstr = DatumGetByteaPP(patt_const->constvalue);
        pattlen = VARSIZE_ANY_EXHDR(bstr);
        patt = (char *) palloc(pattlen);
        memcpy(patt, VARDATA_ANY(bstr), pattlen);
    }

    // Build fixed prefix by scanning until wildcard or case-varying char
    match = palloc(pattlen + 1);
    for (int pos = 0; pos < pattlen; pos++) {
        // Stop at wildcards (% or _)
        if (patt[pos] == '%' || patt[pos] == '_')
            break;

        // Handle escape sequences
        if (patt[pos] == '\\') {
            pos++;
            if (pos >= pattlen) break;
        }

        // Stop at case-varying characters in case-insensitive mode
        if (case_insensitive &&
            pattern_char_isalpha(patt[pos], is_multibyte, locale, locale_is_c))
            break;

        match[match_pos++] = patt[pos];
    }

    match[match_pos] = '\0';

    // Create output prefix constant
    if (typeid != BYTEAOID)
        *prefix_const = string_to_const(match, typeid);
    else
        *prefix_const = string_to_bytea_const(match, match_pos);

    // Calculate remaining pattern selectivity
    if (rest_selec != NULL)
        *rest_selec = like_selectivity(&patt[pos], pattlen - pos, case_insensitive);

    pfree(patt);
    pfree(match);

    // Return prefix status
    if (pos == pattlen)
        return Pattern_Prefix_Exact;    // Entire pattern is literal
    if (match_pos > 0)
        return Pattern_Prefix_Partial;  // Found some prefix
    return Pattern_Prefix_None;         // No usable prefix
}
```