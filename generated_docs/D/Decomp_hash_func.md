# Decomp_hash_func

## Location
[src/include/common/unicode_norm_hashfunc.h:42-2711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/unicode_norm_hashfunc.h#L42-L2711)

## Overview
A perfect hash function used for fast Unicode character decomposition lookups in PostgreSQL's Unicode normalization system.

## Definition

```c
static int
Decomp_hash_func(const void *key)
```
## Detailed Description
 is a static perfect hash function that serves as a key component in PostgreSQL's Unicode normalization functionality. It takes a Unicode character (or character sequence) as input and returns a hash value that can be used to quickly locate the character's decomposition information in the  table.

The function implements a perfect hash algorithm, meaning it provides collision-free mapping for all valid Unicode characters that have decompositions. This ensures O(1) lookup time for Unicode decomposition operations, which is critical for performance in text processing and normalization operations.

The hash function contains a large static lookup table (array  with 13,551 int16 values) that has been pre-computed to provide optimal distribution for Unicode characters requiring decomposition. The values in this table include both positive indices and negative offsets that are used in the hash computation algorithm.

## Parameters / Member Variables
- `*key`: A pointer to the Unicode character or character sequence for which the hash value should be computed
## Dependencies
- Functions called/Symbols referenced:
  - [pg_unicode_decompinfo](../p/pg_unicode_decompinfo.md) (referenced in the same file for decomposition lookups)
- Called from (representative examples):
  - Used internally within Unicode normalization routines for fast character decomposition lookups

## Notes and Other Information
- This is a generated perfect hash function, likely created by an external hash function generator tool (such as gperf)
- The function is marked as , indicating it's only used within the compilation unit where it's defined
- The hash table contains 13,551 entries, suggesting comprehensive coverage of Unicode characters requiring decomposition
- The hash function uses int16 values to keep memory usage reasonable while providing adequate range for hash distribution
- This function is part of PostgreSQL's internal Unicode handling infrastructure and is not exposed to user applications
- The perfect hash property eliminates the need for collision resolution mechanisms, making lookups extremely fast

## Simplified Source

```c
static int
Decomp_hash_func(const void *key) {
    // Large pre-computed hash table with 13,551 entries
    static const int16 h[13551] = { /* ... large lookup table ... */ };

    // Convert input key to bytes for processing
    const unsigned char *k = (const unsigned char *) key;
    size_t keylen = 4;  // Process 4 bytes (Unicode character)

    // Dual hash computation for perfect hashing
    uint32 a = 0;       // First hash accumulator
    uint32 b = 1;       // Second hash accumulator

    // Process each byte of the key
    while (keylen--) {
        unsigned char c = *k++;
        a = a * 257 + c;   // First hash: multiply by 257 and add byte
        b = b * 8191 + c;  // Second hash: multiply by 8191 and add byte
    }

    // Combine hash table lookups to produce final hash value
    return h[a % 13551] + h[b % 13551];
}
```