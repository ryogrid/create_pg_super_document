# NFKC_QC_hash_func

## Location
src/include/common/unicode_normprops_table.h: 6641 - 7926

## Overview
A perfect hash function used to efficiently look up Unicode normalization properties for NFKC (Normalization Form KC - Compatibility Composed) quick check operations.

## Definition

```c
static int
NFKC_QC_hash_func(const void *key)
```
## Detailed Description
This function implements a perfect hash function specifically designed for the NFKC quick check table in PostgreSQL's Unicode normalization system. NFKC normalization is more comprehensive than NFC as it also handles compatibility characters, making it suitable for situations where equivalent representations need to be identified across different character encodings and historical variants.

The function uses the same dual hash approach as NFC_QC_hash_func, with multipliers 257 and 17, but operates on a significantly larger hash table (h[10079] entries vs. h[2463] for NFC). This larger table accommodates the additional complexity introduced by compatibility character mappings in NFKC normalization.

Like its NFC counterpart, this function provides constant-time O(1) lookups with no collisions for the predetermined set of NFKC quick check code points, making Unicode text processing highly efficient.

## Parameters / Member Variables
- : A pointer to a 4-byte Unicode code point value to be hashed for NFKC quick check lookup

## Dependencies
- Functions called/Symbols referenced:
  - pg_unicode_norminfo (referenced in surrounding context)
  - UNICODE_NORM_QC_MAYBE (constant used throughout the hash table)

- Called from (representative examples):
  - Unicode normalization property lookup functions
  - NFKC quick check operations
  - Unicode text processing routines requiring compatibility decomposition

## Notes and Other Information
- This is a static function with internal linkage, accessible only within the same translation unit
- The hash table contains 10079 entries, significantly larger than NFC due to compatibility character mappings
- Uses sentinel value 32767 for unused hash table slots
- Part of PostgreSQL's comprehensive Unicode normalization infrastructure
- Generated automatically from Unicode compatibility data
- NFKC normalization is more aggressive than NFC, handling both canonical and compatibility equivalences
- Essential for applications requiring robust Unicode text matching and searching capabilities