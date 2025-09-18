# varstr_cmp

## Location
src/backend/utils/adt/varlena.c: 1539 - 1593

## Overview
Core string comparison function for text strings with given lengths that provides locale-aware collation support and serves as the foundation for all PostgreSQL text comparison operations.

## Definition


## Detailed Description
 is the fundamental comparison function for variable-length strings in PostgreSQL. It performs locale-aware string comparison while optimizing for the common case where LC_COLLATE is C. The function handles both binary (memcmp) and collation-aware (strcoll) comparisons depending on the specified collation. For non-C locales, it uses a two-phase approach: first performing a quick memcmp for equality detection, then falling back to full collation-aware comparison using pg_strncoll when necessary. For deterministic locales, it provides tie-breaking using binary comparison when collation comparison yields equality.

## Parameters / Member Variables
- : First string to compare (not null-terminated)
- : Length of the first string in bytes
- : Second string to compare (not null-terminated)
- : Length of the second string in bytes  
- : OID of the collation to use for comparison

## Dependencies
- Functions called/Symbols referenced:
  - check_collation_set
  - lc_collate_is_c
  - pg_newlocale_from_collation
  - pg_strncoll
  - pg_locale_deterministic
- Called from (representative examples):
  - text_cmp
  - bpcharcmp
  - namecmp
  - Various text comparison operators

## Notes and Other Information
- Marked as leakproof-compatible, avoiding reporting actual string contents in errors for security
- Optimizes C locale case using direct memcmp for performance
- Uses memory copying strategy for non-C locales due to lack of strncoll() function
- Provides deterministic tie-breaking for equal strings in deterministic locales
- Critical performance path for text indexing and sorting operations