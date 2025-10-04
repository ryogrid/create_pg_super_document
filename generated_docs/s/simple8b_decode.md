# simple8b_decode

## Location
[src/backend/lib/integerset.c:975-1003](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L975-L1003)

## Overview
Decodes a Simple8b compressed 64-bit codeword back into the original sequence of integers, reconstructing values from their stored deltas.

## Definition

```c
static int
simple8b_decode(uint64 codeword, uint64 *decoded, uint64 base)
```
## Detailed Description
This function implements the decoding counterpart to the Simple8b compression algorithm. It takes a 64-bit codeword created by  and reconstructs the original integer sequence by:

1. Extracting the 4-bit selector from bits 60-63 to determine the encoding mode used
2. Looking up the mode parameters (number of integers and bits per integer) from the Simple8b mode table
3. Extracting each delta value from the codeword using bit masking and shifting
4. Reconstructing the original integers by adding each delta to the running sum, starting from the provided base value

The function processes deltas in order (since they were stored in reverse order during encoding) and reconstructs the original integer sequence. Each delta represents the difference minus 1 between consecutive integers, so the reconstruction formula is .

## Parameters / Member Variables
- `codeword`: The 64-bit Simple8b encoded codeword to decode
- `*decoded`: Pointer to array where the decoded integers will be stored
- `base`: The base value that precedes the first encoded integer, used to reconstruct absolute values
## Dependencies
- Functions called/Symbols referenced:
  - : Constant checked to handle empty/invalid codewords
  - : Array containing the decoding mode configurations (referenced implicitly)
- Called from (representative examples):
  -  operations: Used during set traversal and access
  - : Used when iterating through compressed integer sequences in leaf nodes

## Notes and Other Information
- This is a static function, only accessible within integerset.c
- Returns 0 if the codeword is EMPTY_CODEWORD, indicating no integers were encoded
- The decoded array must have sufficient space for the maximum possible number of integers for the given mode
- Reconstructs absolute integer values from the stored delta representation
- The bit mask  extracts exactly the required number of bits for each delta
- Critical component for accessing compressed integer data in IntegerSet B-tree leaf nodes
- Time complexity is O(k) where k is the number of integers encoded in the codeword

## Simplified Source

```c
static int
simple8b_decode(uint64 codeword, uint64 *decoded, uint64 base)
{
    int selector = (codeword >> 60);  // Extract 4-bit mode selector
    int nints = simple8b_modes[selector].num_ints;
    int bits = simple8b_modes[selector].bits_per_int;
    uint64 mask = (UINT64CONST(1) << bits) - 1;
    uint64 curr_value;

    if (codeword == EMPTY_CODEWORD)
        return 0;

    curr_value = base;
    for (int i = 0; i < nints; i++)
    {
        uint64 diff = codeword & mask;  // Extract delta value

        curr_value += 1 + diff;  // Reconstruct original value
        decoded[i] = curr_value;
        codeword >>= bits;  // Move to next delta
    }

    return nints;
}
```