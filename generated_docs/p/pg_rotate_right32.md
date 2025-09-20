# pg_rotate_right32

## Location
[src/include/port/pg_bitutils.h:398-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/pg_bitutils.h#L398-L403)

## Overview
The pg_rotate_right32 function performs a bitwise right rotation of a 32-bit unsigned integer, commonly used in hash functions and cryptographic operations for bit mixing and distribution.

## Definition

```c
static inline uint32
pg_rotate_right32(uint32 word, int n)
```
## Detailed Description
pg_rotate_right32 implements a circular right bit shift operation on a 32-bit unsigned integer. Unlike a regular right shift that fills with zeros, rotation preserves all bits by moving the bits that would be shifted out from the right end to the left end. The operation is performed using bitwise OR of a right shift and left shift: the right shift moves bits right by n positions, while the left shift moves bits left by (32-n) positions, effectively wrapping the shifted-out bits around to the other end.

## Parameters / Member Variables
- `word`: The 32-bit unsigned integer to be rotated
- `n`: The number of bit positions to rotate to the right (should be between 0-31 for proper behavior)

## Dependencies
- Functions called/Symbols referenced:
  - No external function calls (uses only bitwise operations)
- Called from (representative examples):
  - [ExecHashGetBucketAndBatch](../E/ExecHashGetBucketAndBatch.md) (hash table bucket calculation in executor)

## Notes and Other Information
- The function is declared as static inline for performance efficiency
- No bounds checking is performed on the rotation count 'n' - caller must ensure n is within valid range (0-31)
- If n is 0, the function returns the original word unchanged
- If n is greater than 31, the behavior follows standard C shift semantics (typically modulo 32)
- Commonly used in hash functions to improve bit distribution and reduce hash collisions
- The rotation operation is reversible using pg_rotate_left32 with the same count