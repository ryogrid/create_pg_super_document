# qc_hash_lookup

## Location
[src/common/unicode_norm.c:543-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L543-L573)

## Overview
Performs a perfect hash table lookup to find Unicode normalization properties for a given codepoint, used for Quick Check optimization.

## Definition
```c
static const pg_unicode_normprops *qc_hash_lookup(pg_wchar ch, const pg_unicode_norminfo *norminfo)
```

## Detailed Description
This function implements a perfect hash table lookup mechanism to efficiently retrieve Unicode normalization properties for Quick Check operations. The function:

1. Converts the codepoint to network byte order to create a consistent hash key
2. Uses the hash function stored in the norminfo structure to compute the table index  
3. Performs validation to ensure the hash result is within bounds and matches the target codepoint
4. Returns a pointer to the normalization properties if found, or NULL if not found

The perfect hash guarantee means that for any codepoint that has normalization properties, there will be exactly one hash table slot, and false positives are eliminated by the final codepoint comparison.

## Parameters / Member Variables
- `ch`: The Unicode codepoint to look up normalization properties for
- `norminfo`: Pointer to the normalization info structure containing the hash function and properties table

## Dependencies
- Functions called/Symbols referenced:
  - pg_unicode_norminfo (normalization info structure)
  - pg_hton32 (converts 32-bit value to network byte order)
  - [UnicodeNormalizationQC](../U/UnicodeNormalizationQC.md) (related to Quick Check functionality)
- Called from (representative examples):
  - [qc_is_allowed](qc_is_allowed.md) (multiple calls for Quick Check validation)

## Notes and Other Information
- Returns pointer to `pg_unicode_normprops` structure on success, NULL on failure
- Uses perfect hash function for O(1) lookup performance
- Network byte order ensures consistent hashing across different architectures
- Part of the Unicode Quick Check optimization system for normalization
- Critical for efficient normalization form validation without full normalization
- Hash collision handling is unnecessary due to perfect hash properties

## Simplified Source

```c
static const pg_unicode_normprops *qc_hash_lookup(pg_wchar ch, const pg_unicode_norminfo *norminfo) {
    int h;
    uint32 hashkey;

    // Create hash key from codepoint in network byte order
    hashkey = pg_hton32(ch);
    h = norminfo->hash(&hashkey);

    // Check if hash result is valid
    if (h < 0 || h >= norminfo->num_normprops)
        return NULL;

    // Perfect hash: verify this slot matches our codepoint
    if (ch != norminfo->normprops[h].codepoint)
        return NULL;

    // Return the normalization properties
    return &norminfo->normprops[h];
}
```