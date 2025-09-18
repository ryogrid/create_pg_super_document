# varstr_sortsupport

## Location
src/backend/utils/adt/varlena.c: 1873 - 2011

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
  - check_collation_set
  - lc_collate_is_c
  - bpcharfastcmp_c
  - namefastcmp_c
  - varstrfastcmp_c
  - pg_newlocale_from_collation
  - namefastcmp_locale
  - varlenafastcmp_locale
  - pg_strxfrm_enabled
  - initHyperLogLog
  - ssup_datum_unsigned_cmp
  - varstr_abbrev_convert
  - varstr_abbrev_abort
- Called from (representative examples):
  - bttextsortsupport
  - btnamesortsupport
  - bpchar_sortsupport
  - btbpchar_pattern_sortsupport
  - bttext_pattern_sortsupport
  - bytea_sortsupport

## Notes and Other Information
- Assumes that text, VarChar, BpChar, and bytea all have the same internal representation
- Disables abbreviation for NAME type due to implementation limitations  
- Contains platform-specific logic to disable abbreviation for non-C collations where strxfrm() is unreliable
- Uses sophisticated caching with cache_blob flag to distinguish between original strings and strxfrm() transformed data
- Implements HyperLogLog cardinality estimation for abbreviation optimization decisions
- Located in src/backend/utils/adt/varlena.c at lines 1873-2011