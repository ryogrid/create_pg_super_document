# Generic_Text_IC_like

## Location
[src/backend/utils/adt/like.c:171-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L171-L239)

## Overview
A case-insensitive LIKE pattern matching function that handles different character encodings and collations for ILIKE operations in PostgreSQL.

## Definition


## Detailed Description
Generic_Text_IC_like implements case-insensitive pattern matching (ILIKE) with sophisticated locale and encoding awareness. The function employs different strategies based on character encoding and locale provider:

1. **Validation**: Ensures a valid collation is provided and rejects nondeterministic collations
2. **Locale Setup**: Determines if C locale is being used or sets up locale-specific handling
3. **Encoding Strategy Selection**:
   - For multibyte encodings or ICU collations: Pre-converts both pattern and text to lowercase using the  function, then uses UTF8_MatchText or MB_MatchText
   - For single-byte encodings with non-ICU locales: Uses SB_IMatchText with fold-on-the-fly processing for efficiency

The dual approach optimizes performance: single-byte encodings can perform case folding during matching (avoiding memory allocation for lowercased strings), while multibyte encodings require pre-processing due to the complexity of multibyte case conversion.

## Parameters / Member Variables
- : Input text string to match against the pattern
- : ILIKE pattern string containing wildcards and literal characters  
- : OID of the collation to use for case-insensitive comparison

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