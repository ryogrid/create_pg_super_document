# pg_mb_radix_conv

## Location
[src/backend/utils/mb/conv.c:373-506](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conv.c#L373-L506)

## Overview
A static inline function that performs character encoding conversion using a radix tree data structure for efficient lookup of multibyte character mappings.

## Definition
```c
static inline uint32 pg_mb_radix_conv(const pg_mb_radix_tree *rt, int l, unsigned char b1, unsigned char b2, unsigned char b3, unsigned char b4)
```

## Detailed Description
The `pg_mb_radix_conv` function implements a sophisticated character conversion mechanism using a radix tree (prefix tree) data structure. It takes a multibyte character represented by up to 4 bytes and converts it to another encoding using precomputed conversion tables stored in the radix tree structure.

The function handles variable-length multibyte characters (1 to 4 bytes) and uses the radix tree to efficiently navigate through the conversion mappings. For each byte length, it first validates that the input bytes fall within the expected ranges defined in the radix tree, then performs a multi-level lookup through the tree structure to find the corresponding converted character code.

The radix tree can use either 16-bit or 32-bit character arrays depending on the encoding requirements, and the function automatically adapts to the appropriate data type. This design allows for compact storage while maintaining fast O(k) lookup time where k is the number of bytes in the character.

## Parameters / Member Variables
- `rt`: Pointer to the radix tree structure containing conversion mappings and metadata
- `l`: Length of the input character in bytes (1-4)
- `b1`: First byte of the input character (used for 4-byte characters)
- `b2`: Second byte of the input character (used for 3-4 byte characters)
- `b3`: Third byte of the input character (used for 2-4 byte characters) 
- `b4`: Fourth byte of the input character (used for all character lengths)

## Dependencies
- Functions called/Symbols referenced:
  - pg_mb_radix_tree (structure type for radix tree data)
- Called from (representative examples):
  - [UtfToLocal](../U/UtfToLocal.md) (for UTF-8 to local encoding conversion)
  - [LocalToUtf](../L/LocalToUtf.md) (for local encoding to UTF-8 conversion)

## Notes and Other Information
- This is a static inline function optimized for performance in character conversion operations
- Returns 0 if the input character is invalid or not found in the conversion table
- Supports both 16-bit and 32-bit output character codes depending on the radix tree configuration
- The function uses range validation to quickly reject invalid input characters before attempting lookup
- Critical component of PostgreSQLs multibyte character encoding conversion system
- The radix tree structure allows for memory-efficient storage of sparse character conversion mappings
- Byte parameters are arranged with b4 always containing the least significant byte regardless of character length

## Simplified Source

```c
static inline uint32 pg_mb_radix_conv(const pg_mb_radix_tree *rt, int l,
                                      unsigned char b1, unsigned char b2,
                                      unsigned char b3, unsigned char b4) {
    uint32 idx;

    // Handle different character byte lengths
    if (l == 4) {
        // 4-byte character: validate all bytes within expected ranges
        if (b1 < rt->b4_1_lower || b1 > rt->b4_1_upper ||
            b2 < rt->b4_2_lower || b2 > rt->b4_2_upper ||
            b3 < rt->b4_3_lower || b3 > rt->b4_3_upper ||
            b4 < rt->b4_4_lower || b4 > rt->b4_4_upper)
            return 0;  // Invalid character

        // Navigate radix tree: start at root, traverse each byte level
        idx = rt->b4root;
        if (rt->chars32) {
            idx = rt->chars32[b1 + idx - rt->b4_1_lower];
            idx = rt->chars32[b2 + idx - rt->b4_2_lower];
            idx = rt->chars32[b3 + idx - rt->b4_3_lower];
            return rt->chars32[b4 + idx - rt->b4_4_lower];
        } else {
            idx = rt->chars16[b1 + idx - rt->b4_1_lower];
            idx = rt->chars16[b2 + idx - rt->b4_2_lower];
            idx = rt->chars16[b3 + idx - rt->b4_3_lower];
            return rt->chars16[b4 + idx - rt->b4_4_lower];
        }
    }
    else if (l == 3) {
        // 3-byte character: similar validation and lookup
        if (b2 < rt->b3_1_lower || b2 > rt->b3_1_upper ||
            b3 < rt->b3_2_lower || b3 > rt->b3_2_upper ||
            b4 < rt->b3_3_lower || b4 > rt->b3_3_upper)
            return 0;

        idx = rt->b3root;
        if (rt->chars32) {
            idx = rt->chars32[b2 + idx - rt->b3_1_lower];
            idx = rt->chars32[b3 + idx - rt->b3_2_lower];
            return rt->chars32[b4 + idx - rt->b3_3_lower];
        } else {
            idx = rt->chars16[b2 + idx - rt->b3_1_lower];
            idx = rt->chars16[b3 + idx - rt->b3_2_lower];
            return rt->chars16[b4 + idx - rt->b3_3_lower];
        }
    }
    else if (l == 2) {
        // 2-byte character
        if (b3 < rt->b2_1_lower || b3 > rt->b2_1_upper ||
            b4 < rt->b2_2_lower || b4 > rt->b2_2_upper)
            return 0;

        idx = rt->b2root;
        if (rt->chars32) {
            idx = rt->chars32[b3 + idx - rt->b2_1_lower];
            return rt->chars32[b4 + idx - rt->b2_2_lower];
        } else {
            idx = rt->chars16[b3 + idx - rt->b2_1_lower];
            return rt->chars16[b4 + idx - rt->b2_2_lower];
        }
    }
    else if (l == 1) {
        // 1-byte character: direct lookup
        if (b4 < rt->b1_lower || b4 > rt->b1_upper)
            return 0;

        if (rt->chars32)
            return rt->chars32[b4 + rt->b1root - rt->b1_lower];
        else
            return rt->chars16[b4 + rt->b1root - rt->b1_lower];
    }

    return 0;  // Invalid length
}
```