# varstr_cmp

## Location
[src/backend/utils/adt/varlena.c:1539-1593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1539-L1593)

## Overview
Core string comparison function for text strings with given lengths that provides locale-aware collation support and serves as the foundation for all PostgreSQL text comparison operations.

## Definition

```c
int
varstr_cmp(const char *arg1, int len1, const char *arg2, int len2, Oid collid)
```
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
  - [check_collation_set](../c/check_collation_set.md)
  - [lc_collate_is_c](../l/lc_collate_is_c.md)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)
  - [pg_strncoll](../p/pg_strncoll.md)
  - [pg_locale_deterministic](../p/pg_locale_deterministic.md)
- Called from (representative examples):
  - [text_cmp](../t/text_cmp.md)
  - [bpcharcmp](../b/bpcharcmp.md)
  - namecmp
  - Various text comparison operators

## Notes and Other Information
- Marked as leakproof-compatible, avoiding reporting actual string contents in errors for security
- Optimizes C locale case using direct memcmp for performance
- Uses memory copying strategy for non-C locales due to lack of strncoll() function
- Provides deterministic tie-breaking for equal strings in deterministic locales
- Critical performance path for text indexing and sorting operations