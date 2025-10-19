# hashint2extended

## Location
[src/backend/access/hash/hashfunc.c:65-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L65-L70)

## Overview
hashint2extended is an extended hash function for the int2 (16-bit integer) data type in PostgreSQL that accepts an additional seed parameter for enhanced hash computation.

## Definition
```c
Datum hashint2extended(PG_FUNCTION_ARGS)
```

## Detailed Description
The hashint2extended function provides an extended version of the hashint2 function, supporting seeded hash computation for int2 (16-bit signed integer) data types. This function is part of PostgreSQL's extended hash function family that allows for hash value computation with a user-provided seed value, which is useful for applications requiring hash randomization or multi-level hashing schemes.

The function extracts both a 16-bit integer argument and a 64-bit integer seed from the function call context, then delegates the actual hashing to hash_uint32_extended after casting the 16-bit value to a 32-bit integer.

## Parameters / Member Variables
- First argument: int2 (16-bit integer) value accessed via PG_GETARG_INT16(0)
- Second argument: int64 seed value accessed via PG_GETARG_INT64(1)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_uint32_extended](hash_uint32_extended.md): Extended hash function for 32-bit unsigned integers with seed support
  - PG_GETARG_INT16: Macro to extract 16-bit integer argument from function call context
  - PG_GETARG_INT64: Macro to extract 64-bit integer argument from function call context

- Called from (representative examples):
  - No direct references found in the analyzed codebase (likely referenced through function pointers in system catalogs)

## Notes and Other Information
- Extended version of hashint2 function with seed parameter support
- Enables hash randomization and seeded hash computations for 16-bit integers
- The function promotes the 16-bit input to 32-bit before hashing for consistency with the underlying extended hash algorithm
- Returns a Datum containing the computed hash value
- Part of PostgreSQL's comprehensive extended hash function collection for various data types
- Designed for advanced hashing scenarios that require seed-based hash computation

## Simplified Source

```c
Datum
hashint2extended(PG_FUNCTION_ARGS)
{
    // Extract 16-bit integer and seed, promote to 32-bit and hash with extended function
    return hash_uint32_extended((int32) PG_GETARG_INT16(0), PG_GETARG_INT64(1));
}
```