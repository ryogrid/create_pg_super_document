# Generic_Text_IC_like

## Location
[src/backend/utils/adt/like.c:171-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L171-L239)

## Overview
A case-insensitive LIKE pattern matching function that handles different character encodings and collations for ILIKE operations in PostgreSQL.

## Definition

```c
static inline int
Generic_Text_IC_like(text *str, text *pat, Oid collation)
```
## Detailed Description
Generic_Text_IC_like implements case-insensitive pattern matching (ILIKE) with sophisticated locale and encoding awareness. The function employs different strategies based on character encoding and locale provider:

1. **Validation**: Ensures a valid collation is provided and rejects nondeterministic collations
2. **Locale Setup**: Determines if C locale is being used or sets up locale-specific handling
3. **Encoding Strategy Selection**:
   - For multibyte encodings or ICU collations: Pre-converts both pattern and text to lowercase using the  function, then uses UTF8_MatchText or MB_MatchText
   - For single-byte encodings with non-ICU locales: Uses SB_IMatchText with fold-on-the-fly processing for efficiency

The dual approach optimizes performance: single-byte encodings can perform case folding during matching (avoiding memory allocation for lowercased strings), while multibyte encodings require pre-processing due to the complexity of multibyte case conversion.

## Parameters / Member Variables
- `*str`: Input text string to match against the pattern
- `*pat`: ILIKE pattern string containing wildcards and literal characters
- `collation`: OID of the collation to use for case-insensitive comparison
## Dependencies
- Functions called/Symbols referenced:
  - [lc_ctype_is_c](../l/lc_ctype_is_c.md) (check if collation uses C locale)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md) (create locale from collation)
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md) (validate locale determinism)
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md) (get max bytes per character)
  - [DirectFunctionCall1Coll](../D/DirectFunctionCall1Coll.md) (call lower() function with collation)
  - DatumGetTextPP (extract text from Datum)
  - [GetDatabaseEncoding](GetDatabaseEncoding.md) (get current database encoding)
  - UTF8_MatchText (UTF-8 optimized matching)
  - MB_MatchText (general multibyte matching)
  - SB_IMatchText (single-byte case-insensitive matching)
- Called from (representative examples):
  - [nameiclike](../n/nameiclike.md) (name ILIKE pattern matching)
  - [nameicnlike](../n/nameicnlike.md) (name NOT ILIKE pattern matching)
  - [texticlike](../t/texticlike.md) (text ILIKE pattern matching)
  - [texticnlike](../t/texticnlike.md) (text NOT ILIKE pattern matching)

## Notes and Other Information
- This function implements PostgreSQL's ILIKE operator, which is a case-insensitive version of LIKE
- Requires explicit collation specification and reports helpful error messages when collation cannot be determined
- The choice between pre-lowercasing vs fold-on-the-fly is driven by encoding complexity and ICU limitations
- ICU (International Components for Unicode) provider requires the pre-lowercasing approach because it doesn't support single-character case folding
- Part of PostgreSQL's comprehensive internationalization support while maintaining optimal performance for common single-byte cases
- Returns LIKE_TRUE, LIKE_FALSE, or LIKE_ABORT depending on match result

## Simplified Source

```c
static inline int
Generic_Text_IC_like(text *str, text *pat, Oid collation)
{
    char *s, *p;
    int slen, plen;
    pg_locale_t locale = 0;
    bool locale_is_c = false;

    // Validate collation is provided
    if (!OidIsValid(collation)) {
        ereport(ERROR,
                (errcode(ERRCODE_INDETERMINATE_COLLATION),
                 errmsg("could not determine which collation to use for ILIKE"),
                 errhint("Use the COLLATE clause to set the collation explicitly.")));
    }

    // Set up locale handling
    if (lc_ctype_is_c(collation))
        locale_is_c = true;
    else
        locale = pg_newlocale_from_collation(collation);

    // Check for deterministic collation
    if (!pg_locale_deterministic(locale))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("nondeterministic collations are not supported for ILIKE")));

    // Choose strategy based on encoding and locale provider
    if (pg_database_encoding_max_length() > 1 || (locale && locale->provider == COLLPROVIDER_ICU)) {
        // Multibyte or ICU: pre-convert to lowercase, then match
        pat = DatumGetTextPP(DirectFunctionCall1Coll(lower, collation, PointerGetDatum(pat)));
        str = DatumGetTextPP(DirectFunctionCall1Coll(lower, collation, PointerGetDatum(str)));

        p = VARDATA_ANY(pat);
        plen = VARSIZE_ANY_EXHDR(pat);
        s = VARDATA_ANY(str);
        slen = VARSIZE_ANY_EXHDR(str);

        if (GetDatabaseEncoding() == PG_UTF8)
            return UTF8_MatchText(s, slen, p, plen, 0, true);
        else
            return MB_MatchText(s, slen, p, plen, 0, true);
    } else {
        // Single-byte: use fold-on-the-fly for efficiency
        p = VARDATA_ANY(pat);
        plen = VARSIZE_ANY_EXHDR(pat);
        s = VARDATA_ANY(str);
        slen = VARSIZE_ANY_EXHDR(str);

        return SB_IMatchText(s, slen, p, plen, locale, locale_is_c);
    }
}
```