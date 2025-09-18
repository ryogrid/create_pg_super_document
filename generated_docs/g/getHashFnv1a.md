# getHashFnv1a

## Location
[src/bin/pgbench/pgbench.c:1245-1269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1245-L1269)

## Overview
Implements the FNV-1a (Fowler-Noll-Vo) hash function variant that produces a 64-bit hash value from an input integer and seed.

## Definition
```c
static int64 getHashFnv1a(int64 val, uint64 seed)
```

## Detailed Description
This function implements the FNV-1a hash algorithm, a non-cryptographic hash function known for good distribution properties and simplicity. The algorithm starts with the FNV offset basis XORed with a seed value, then processes the input value byte by byte (8 bytes total for int64). For each byte, it XORs the hash result with the byte value, then multiplies by the FNV prime constant. This variant (FNV-1a) differs from standard FNV-1 by performing the XOR operation before the multiplication, which typically provides better avalanche characteristics.

## Parameters / Member Variables
- `val`: The 64-bit integer value to be hashed
- `seed`: A 64-bit seed value for hash initialization, XORed with the FNV offset basis

## Dependencies
- Functions called/Symbols referenced:
  - FNV_OFFSET_BASIS (constant)
  - FNV_PRIME (constant)
- Called from (representative examples):
  - evalStandardFunc

## Notes and Other Information
- Processes input value byte-by-byte using bit shifting and masking operations
- Uses FNV-1a variant which XORs before multiplication for better distribution
- Non-cryptographic hash function suitable for hash tables and checksums
- Returns signed int64 result despite hash nature (common in PostgreSQL codebase)
- Part of pgbench's standard function evaluation system for generating hash-based values
- Located in src/bin/pgbench/pgbench.c:1245-1269