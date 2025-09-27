# rotl

## Location
[src/common/pg_prng.c:41-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_prng.c#L41-L53)

## Overview
The `rotl` function performs a 64-bit left rotation operation on an unsigned 64-bit integer, used as a building block for the xoroshiro128** pseudo-random number generator algorithm.

## Definition
```c
static inline uint64 rotl(uint64 x, int bits)
```

## Detailed Description
The `rotl` function implements a bitwise left rotation (circular shift) operation on a 64-bit unsigned integer. Unlike a regular left shift which discards the most significant bits, rotation preserves all bits by moving the bits that would be shifted out from the left end to the right end. This operation is essential for the xoroshiro128** algorithm, which relies on bit rotation to achieve good statistical properties in pseudo-random number generation.

The function is declared as `static inline`, making it an internal utility function that is optimized for performance through inlining. The rotation is implemented using bit shifting and OR operations: the left part shifts the bits left by the specified amount, while the right part captures the bits that would overflow and places them at the right end.

## Parameters / Member Variables
- `x`: The 64-bit unsigned integer value to be rotated
- `bits`: The number of bit positions to rotate left (must be between 0-63 for meaningful results)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only bitwise operations)
- Called from (representative examples):
  - [xoroshiro128ss](../x/xoroshiro128ss.md) (called 3 times within the xoroshiro128** algorithm)

## Notes and Other Information
- This is a static inline function, meaning it is only accessible within the same compilation unit and is optimized for performance
- The rotation count should be between 0-63; values outside this range will produce undefined or unexpected behavior
- This function is specifically designed to support the xoroshiro128** pseudo-random number generator algorithm
- The implementation assumes the compiler will optimize the bit operations efficiently
- Located in src/common/pg_prng.c, which contains PostgreSQL's pseudo-random number generation utilities

## Simplified Source

```c
// Simplified version of rotl
static inline uint64 rotl(uint64 x, int bits) {
    // Perform 64-bit left rotation by combining left shift with right shift
    // Left part: shift bits left
    // Right part: capture overflow bits and place them at the right end
    return (x << bits) | (x >> (64 - bits));
}
```

Key simplifications made:
- Added clear comments explaining the rotation operation
- Explained how the left and right shift operations work together
- This function is already very simple and efficient
- Preserved the essential bit manipulation logic
- Maintained the inline declaration for performance