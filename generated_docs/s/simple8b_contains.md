# simple8b_contains

## Location
[src/backend/lib/integerset.c:1004-1042](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L1004-L1042)

## Overview
A static function that checks if a given key value is present in a Simple-8b compressed codeword without fully decoding all values, providing an efficient membership test for integer sets.

## Definition
static bool simple8b_contains(uint64 codeword, uint64 key, uint64 base)

## Detailed Description
The simple8b_contains function is a specialized version of Simple-8b decoding that performs membership testing without the overhead of decoding all values in a codeword. It uses the Simple-8b compression scheme, which packs multiple small integers into a single 64-bit word by storing their differences from a base value.

The function extracts the selector (top 4 bits) from the codeword to determine the encoding mode, then iteratively processes the packed values to check if any match the target key. It handles two main cases:
1. Zero-bit encoding: When all values in the range can be represented implicitly
2. Variable-bit encoding: When values are explicitly stored as differences from the base

The algorithm stops early when it finds the target value or determines it cannot be present (when the current decoded value exceeds the target), making it more efficient than full decoding for membership testing.

## Parameters / Member Variables
- codeword: The 64-bit Simple-8b encoded word containing packed integer differences
- key: The target value to search for within the codeword
- base: The base value from which all packed differences are calculated

## Dependencies
- Functions called/Symbols referenced:
  - EMPTY_CODEWORD (constant for indicating empty codewords)
  - simple8b_modes (array defining encoding parameters for each selector mode)

- Called from (representative examples):
  - [intset_is_member](../i/intset_is_member.md) (main entry point for integer set membership testing)

## Notes and Other Information
- This is a static function internal to the integerset module, not exposed in the public API
- The function is optimized for early termination - it returns as soon as it determines whether the key is present
- Special handling for EMPTY_CODEWORD ensures correct behavior for empty sets
- The zero-bit case handles scenarios where all values in a range can be encoded implicitly without storing actual bits
- Used as part of PostgreSQL's IntegerSet data structure for efficient storage and querying of large sets of integers
- The Simple-8b encoding scheme is particularly effective for storing sequences of integers with small gaps between values

## Simplified Source

```c
static bool
simple8b_contains(uint64 codeword, uint64 key, uint64 base)
{
    int selector = (codeword >> 60);  // Extract 4-bit mode selector
    int nints = simple8b_modes[selector].num_ints;
    int bits = simple8b_modes[selector].bits_per_int;

    if (codeword == EMPTY_CODEWORD)
        return false;

    if (bits == 0)
    {
        // Special case: 0-bit encoding (consecutive values)
        return (key - base) <= nints;
    }
    else
    {
        uint64 mask = (UINT64CONST(1) << bits) - 1;
        uint64 curr_value;

        curr_value = base;
        for (int i = 0; i < nints; i++)
        {
            uint64 diff = codeword & mask;  // Extract delta value

            curr_value += 1 + diff;  // Reconstruct original value

            if (curr_value >= key)
            {
                return (curr_value == key);  // Found match or passed target
            }

            codeword >>= bits;  // Move to next delta
        }
    }

    return false;
}
```