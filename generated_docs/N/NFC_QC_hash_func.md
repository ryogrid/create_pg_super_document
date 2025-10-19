# NFC_QC_hash_func

## Location
[src/include/common/unicode_normprops_table.h:1262-6640](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/unicode_normprops_table.h#L1262-L6640)

## Overview
A perfect hash function used to efficiently look up Unicode normalization properties for NFC (Normalization Form Composed) quick check operations.

## Definition

```c
static int
NFC_QC_hash_func(const void *key)
```
## Detailed Description
This function implements a perfect hash function specifically designed for the NFC quick check table in PostgreSQL's Unicode normalization system. Perfect hash functions provide constant-time O(1) lookups with no collisions for a predetermined set of keys, making them ideal for static Unicode property tables.

The function uses a dual hash approach with two different multipliers (257 and 17) to compute hash values that are then used to index into a large hash table (h[2463]) containing precomputed values. This hash table was generated offline to ensure perfect hashing for all NFC quick check code points.

The function operates on 4-byte Unicode code point keys and returns an integer hash value that can be used to locate the corresponding normalization properties in the Unicode property tables.

## Parameters / Member Variables
- `*key`: A pointer to a 4-byte Unicode code point value to be hashed
## Dependencies
- Functions called/Symbols referenced:
  - pg_unicode_norminfo (referenced in surrounding context)
  - [pg_unicode_normprops](../p/pg_unicode_normprops.md) (referenced in surrounding context)
  - UNICODE_NORM_QC_MAYBE (constant used extensively in the hash table)

- Called from (representative examples):
  - Unicode normalization property lookup functions
  - NFC quick check operations

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the same translation unit
- The hash table contains 2463 entries with precomputed values, including special sentinel value 32767
- The dual hash computation (using both multipliers 257 and 17) helps distribute keys more evenly
- Part of PostgreSQL's comprehensive Unicode normalization support infrastructure
- Generated automatically from Unicode data, not hand-written
- Used specifically for NFC (Normalization Form Composed) operations, complementing similar functions for other normalization forms

## Simplified Source

```c
static int
NFC_QC_hash_func(const void *key) {
    // Pre-computed hash table with 2,463 entries for NFC quick check
    static const int16 h[2463] = { /* ... lookup table ... */ };

    // Convert input key to bytes for processing
    const unsigned char *k = (const unsigned char *) key;
    size_t keylen = 4;  // Process 4 bytes (Unicode code point)

    // Dual hash computation for perfect hashing
    uint32 a = 0;       // First hash accumulator (multiplier: 257)
    uint32 b = 0;       // Second hash accumulator (multiplier: 17)

    // Process each byte of the Unicode code point
    while (keylen--) {
        unsigned char c = *k++;
        a = a * 257 + c;   // First hash calculation
        b = b * 17 + c;    // Second hash calculation
    }

    // Combine results from both hash table lookups
    return h[a % 2463] + h[b % 2463];
}
```