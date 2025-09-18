# hashfloat8

## Location
src/backend/access/hash/hashfunc.c: 193 - 216

## Overview
A PostgreSQL hash function for 8-byte floating-point values that handles IEEE floating-point special cases like zero, negative zero, and NaN values for consistent hashing behavior.

## Definition
```c
Datum hashfloat8(PG_FUNCTION_ARGS)
```

## Detailed Description
The `hashfloat8` function computes hash values for double-precision floating-point numbers (float8) with careful handling of IEEE floating-point special cases. The function first checks for zero values (including negative zero) and returns a consistent hash value of 0 to ensure that mathematically equivalent values hash identically. For NaN values, which can have different bit patterns but should be considered equal, the function normalizes them to a standard float8 NaN representation using `get_float8_nan()`. The final hash computation uses the generic `hash_any` function on the 8-byte representation of the normalized value.

## Parameters / Member Variables
- Uses PostgreSQL's function argument macros (`PG_FUNCTION_ARGS`)
- `key`: The input float8 value obtained via `PG_GETARG_FLOAT8(0)`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8
  - PG_RETURN_UINT32
  - isnan
  - get_float8_nan
  - hash_any
- Called from (representative examples):
  - tablesample_init (at src/backend/executor/nodeSamplescan.c:270)

## Notes and Other Information
- Handles IEEE floating-point special cases: zero/negative zero are normalized to hash value 0
- NaN values are normalized to a standard float8 NaN representation for consistency across different bit patterns
- This is the reference implementation that `hashfloat4` uses for cross-type compatibility
- Uses `hash_any` on the full 8-byte float8 representation for the hash computation
- Located in src/backend/access/hash/hashfunc.c at lines 193-216
- Essential for hash indexes and hash joins involving float8 columns
- Referenced by table sampling operations for hash-based sampling algorithms