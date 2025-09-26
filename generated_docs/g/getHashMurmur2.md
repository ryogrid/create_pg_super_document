# getHashMurmur2

## Location
[src/bin/pgbench/pgbench.c:1270-1302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1270-L1302)

## Overview
Implements the Murmur2 hash function that produces a 64-bit hash value from an input integer and seed, based on Austin Appleby's original work.

## Definition
```c
static int64 getHashMurmur2(int64 val, uint64 seed)
```

## Detailed Description
This function implements the Murmur2 hash algorithm, a non-cryptographic hash function developed by Austin Appleby. The algorithm is known for excellent distribution properties, speed, and simplicity. It begins by initializing the result with a seed XORed with a constant (MM2_MUL_TIMES_8). The input value undergoes a series of multiply, rotate, and XOR operations using specific constants (MM2_MUL, MM2_ROT) designed to provide good avalanche effects. The final result goes through additional finalization steps to ensure all bits are well-mixed.

## Parameters / Member Variables
- `val`: The 64-bit integer value to be hashed
- `seed`: A 64-bit seed value for hash initialization

## Dependencies
- Functions called/Symbols referenced:
  - MM2_MUL_TIMES_8 (constant)
  - MM2_MUL (constant) 
  - MM2_ROT (constant)
- Called from (representative examples):
  - [evalStandardFunc](../e/evalStandardFunc.md)

## Notes and Other Information
- Based on Austin Appleby's MurmurHash2 from SMHasher project
- Uses specific multiplication and rotation constants for optimal mixing
- Includes finalization steps to ensure good bit distribution
- Non-cryptographic hash suitable for hash tables and data structures
- Returns signed int64 despite being a hash function (common in PostgreSQL)
- Part of pgbench's standard function evaluation system
- Reference implementation available at https://github.com/aappleby/smhasher
- Located in src/bin/pgbench/pgbench.c:1270-1302