# GenericMatchText

## Location
[src/backend/utils/adt/like.c:150-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like.c#L150-L170)

## Overview
A dispatcher function that selects the appropriate character encoding-specific LIKE pattern matching implementation based on database encoding and collation settings.

## Definition


## Detailed Description
GenericMatchText serves as a smart dispatcher in PostgreSQL's LIKE pattern matching system. It analyzes the current database configuration and selects the most appropriate matching algorithm:

1. First, it validates that nondeterministic collations are not used (which are unsupported for LIKE operations)
2. For single-byte encodings: delegates to SB_MatchText for optimal single-byte performance
3. For UTF-8 encoding: uses UTF8_MatchText for UTF-8 specific optimizations
4. For other multibyte encodings: falls back to MB_MatchText for general multibyte support

This design allows PostgreSQL to provide optimal performance for each encoding type while maintaining a unified interface for LIKE operations. The function ensures that collation requirements are met while routing to the most efficient implementation available.

## Parameters / Member Variables
- : Input text string to match against the pattern
- : Length of the input text string in bytes
- : LIKE pattern string containing wildcards and literal characters
- : Length of the pattern string in bytes
- : OID of the collation to use for comparison operations

## Dependencies
- Functions called/Symbols referenced:
  - [lc_ctype_is_c](../l/lc_ctype_is_c.md) (check if collation is C locale)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md) (create locale from collation)
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md) (check if locale is deterministic)
  - [pg_database_encoding_max_length](../p/pg_database_encoding_max_length.md) (get max bytes per character)
  - [GetDatabaseEncoding](GetDatabaseEncoding.md) (get current database encoding)
  - SB_MatchText (single-byte matching)
  - UTF8_MatchText (UTF-8 specific matching)  
  - MB_MatchText (multibyte matching)
- Called from (representative examples):
  - [namelike](../n/namelike.md) (name LIKE pattern matching)
  - [namenlike](../n/namenlike.md) (name NOT LIKE pattern matching)
  - [textlike](../t/textlike.md) (text LIKE pattern matching)
  - [textnlike](../t/textnlike.md) (text NOT LIKE pattern matching)

## Notes and Other Information
- This function is part of PostgreSQL's multi-tier architecture for LIKE pattern matching that optimizes performance based on character encoding
- Explicitly rejects nondeterministic collations with an error, ensuring predictable LIKE behavior
- The dispatch logic prioritizes performance: single-byte > UTF-8 optimized > general multibyte
- Returns LIKE_TRUE, LIKE_FALSE, or LIKE_ABORT depending on match result
- The inline keyword suggests this function is performance-critical and should be inlined by the compiler
- Part of the generic case handling that doesn't require inline case-folding (as noted in the source comment)