# hashfloat4extended

## Location
[src/backend/access/hash/hashfunc.c:176-192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L176-L192)

## Overview
An extended PostgreSQL hash function for 4-byte floating-point values that supports a seed parameter while maintaining the same special case handling and cross-type compatibility as hashfloat4.

## Definition
```c
Datum hashfloat4extended(PG_FUNCTION_ARGS)
```

## Detailed Description
The `hashfloat4extended` function is the extended version of `hashfloat4` that supports seeded hashing. It follows the same approach as `hashfloat4` for handling IEEE floating-point special cases but incorporates a 64-bit seed value. For zero values (including negative zero), it returns the seed directly. For non-zero values, it widens the float4 to float8 for cross-type compatibility, normalizes NaN values to a standard representation, and then uses `hash_any_extended` with the seed parameter to compute the final hash value.

## Parameters / Member Variables
- `key`: The input float4 value obtained via `PG_GETARG_FLOAT4(0)`
- `seed`: The 64-bit seed value obtained via `PG_GETARG_INT64(1)`
- `key8`: The widened float8 representation used for consistent hashing

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4
  - PG_GETARG_INT64
  - PG_RETURN_UINT64
  - isnan
  - [get_float8_nan](../g/get_float8_nan.md)
  - [hash_any_extended](hash_any_extended.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Extended version of `hashfloat4` that supports seeded hashing operations
- For zero values, returns the seed directly instead of computing a hash
- Maintains the same special case handling as `hashfloat4`: zero normalization and NaN standardization
- Ensures cross-type hashing compatibility by widening float4 to float8 before hashing
- Uses `hash_any_extended` for the final hash computation with seed incorporation
- Located in src/backend/access/hash/hashfunc.c at lines 176-192
- Useful for hash partitioning and distributed hash operations involving float4 values