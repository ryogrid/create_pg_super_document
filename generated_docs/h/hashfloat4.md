# hashfloat4

## Location
[src/backend/access/hash/hashfunc.c:140-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L140-L175)

## Overview
A PostgreSQL hash function for 4-byte floating-point values that handles special cases like zero, negative zero, and NaN values while ensuring cross-type compatibility with float8 hashing.

## Definition
```c
Datum hashfloat4(PG_FUNCTION_ARGS)
```

## Detailed Description
The `hashfloat4` function computes hash values for single-precision floating-point numbers (float4) with careful handling of IEEE floating-point special cases. The function first checks for zero values (including negative zero) and returns a consistent hash value of 0. For non-zero values, it widens the float4 to float8 to ensure cross-type hashing compatibility - meaning that equivalent float4 and float8 values produce the same hash. It also normalizes NaN values to a standard float8 NaN representation before hashing. The final hash computation uses the generic `hash_any` function on the 8-byte representation.

## Parameters / Member Variables
- `key`: The input float4 value obtained via `PG_GETARG_FLOAT4(0)`
- `key8`: The widened float8 representation used for consistent hashing

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4
  - PG_RETURN_UINT32
  - isnan
  - [get_float8_nan](../g/get_float8_nan.md)
  - [hash_any](hash_any.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Handles IEEE floating-point special cases: zero/negative zero are normalized to hash value 0
- NaN values are normalized to a standard float8 NaN representation for consistency
- Ensures cross-type hashing compatibility by widening float4 to float8 before hashing
- Uses `hash_any` on the 8-byte float8 representation rather than the original 4-byte value
- Located in src/backend/access/hash/hashfunc.c at lines 140-175
- Critical for hash indexes and hash joins involving float4 columns

## Simplified Source
```c
Datum hashfloat4(PG_FUNCTION_ARGS) {
    float4 key = PG_GETARG_FLOAT4(0);
    float8 key8;

    // Handle zero case: both positive and negative zero hash to 0
    if (key == (float4) 0)
        PG_RETURN_UINT32(0);

    // Widen to float8 for cross-type hashing compatibility
    key8 = key;

    // Normalize NaN values to standard float8 NaN
    if (isnan(key8))
        key8 = get_float8_nan();

    return hash_any((unsigned char *) &key8, sizeof(key8));
}
```