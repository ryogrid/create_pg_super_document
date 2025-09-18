# varstr_sortsupport

## Location
[src/backend/utils/adt/varlena.c:1873-2011](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1873-L2011)

## Overview
A generic sort support interface for character types that provides optimized sorting functionality with locale support and abbreviation capabilities for various string data types.

## Definition


## Detailed Description
The `varstr_sortsupport` function is the central implementation for character type sorting support in PostgreSQL. It provides a unified interface for sorting text, varchar, bpchar, and bytea data types by selecting the most appropriate comparison function based on collation settings and data type. The function implements several optimizations including direct datum comparison to avoid function manager overhead, locale-aware comparisons using strcoll(), and C-locale fast comparisons using memcmp(). It also supports abbreviation optimization for improved performance on large datasets, with sophisticated caching mechanisms to handle interleaved comparisons and conversions efficiently.

## Parameters / Member Variables
- `ssup`: SortSupport structure containing sort configuration and context information
- `typid`: OID of the data type being sorted (TEXTOID, BPCHAROID, NAMEOID, etc.)
- `collid`: Collation identifier specifying the locale and comparison rules to use

## Dependencies
- Functions called/Symbols referenced:
  - [check_collation_set](../c/check_collation_set.md)
  - [lc_collate_is_c](../l/lc_collate_is_c.md)
  - [bpcharfastcmp_c](../b/bpcharfastcmp_c.md)
  - [namefastcmp_c](../n/namefastcmp_c.md)
  - [varstrfastcmp_c](varstrfastcmp_c.md)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)
  - [namefastcmp_locale](../n/namefastcmp_locale.md)
  - [varlenafastcmp_locale](varlenafastcmp_locale.md)
  - [pg_strxfrm_enabled](../p/pg_strxfrm_enabled.md)
  - initHyperLogLog
  - ssup_datum_unsigned_cmp
  - [varstr_abbrev_convert](varstr_abbrev_convert.md)
  - [varstr_abbrev_abort](varstr_abbrev_abort.md)
- Called from (representative examples):
  - [bttextsortsupport](../b/bttextsortsupport.md)
  - btnamesortsupport
  - [bpchar_sortsupport](../b/bpchar_sortsupport.md)
  - [btbpchar_pattern_sortsupport](../b/btbpchar_pattern_sortsupport.md)
  - [bttext_pattern_sortsupport](../b/bttext_pattern_sortsupport.md)
  - [bytea_sortsupport](../b/bytea_sortsupport.md)

## Notes and Other Information
- Assumes that text, VarChar, BpChar, and bytea all have the same internal representation
- Disables abbreviation for NAME type due to implementation limitations  
- Contains platform-specific logic to disable abbreviation for non-C collations where strxfrm() is unreliable
- Uses sophisticated caching with cache_blob flag to distinguish between original strings and strxfrm() transformed data
- Implements HyperLogLog cardinality estimation for abbreviation optimization decisions
- Located in src/backend/utils/adt/varlena.c at lines 1873-2011