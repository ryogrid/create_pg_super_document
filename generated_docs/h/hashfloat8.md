# hashfloat8

## Location
[src/backend/access/hash/hashfunc.c:193-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L193-L216)

## Overview
A PostgreSQL hash function for 8-byte floating-point values that handles IEEE floating-point special cases like zero, negative zero, and NaN values for consistent hashing behavior.

## Definition
```c
Datum hashfloat8(PG_FUNCTION_ARGS)
```

## Detailed Description
The `hashfloat8` function computes hash values for double-precision floating-point numbers (float8) with careful handling of IEEE floating-point special cases. The function first checks for zero values (including negative zero) and returns a consistent hash value of 0 to ensure that mathematically equivalent values hash identically. For NaN values, which can have different bit patterns but should be considered equal, the function normalizes them to a standard float8 NaN representation using `get_float8_nan()`. The final hash computation uses the generic `hash_any` function on the 8-byte representation of the normalized value.

## Parameters / Member Variables
- `key`: The input float8 value obtained via `PG_GETARG_FLOAT8(0)`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8
  - PG_RETURN_UINT32
  - isnan
  - [get_float8_nan](../g/get_float8_nan.md)
  - [hash_any](hash_any.md)
- Called from (representative examples):
  - [tablesample_init](../t/tablesample_init.md) (at src/backend/executor/nodeSamplescan.c:270)

## Notes and Other Information
- Handles IEEE floating-point special cases: zero/negative zero are normalized to hash value 0
- NaN values are normalized to a standard float8 NaN representation for consistency across different bit patterns
- This is the reference implementation that `hashfloat4` uses for cross-type compatibility
- Uses `hash_any` on the full 8-byte float8 representation for the hash computation
- Located in src/backend/access/hash/hashfunc.c at lines 193-216
- Essential for hash indexes and hash joins involving float8 columns
- Referenced by table sampling operations for hash-based sampling algorithms

## Simplified Source

```c
Datum hashfloat8(PG_FUNCTION_ARGS) {
    float8 key = PG_GETARG_FLOAT8(0);

    // Handle zero values: normalize -0.0 and +0.0 to same hash value
    if (key == (float8) 0) {
        PG_RETURN_UINT32(0);
    }

    // Handle NaN values: normalize all NaN bit patterns to standard NaN
    if (isnan(key)) {
        key = get_float8_nan();
    }

    // Compute hash on the normalized 8-byte float representation
    return hash_any((unsigned char *) &key, sizeof(key));
}
```