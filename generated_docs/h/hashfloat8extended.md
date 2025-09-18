# hashfloat8extended

## Location
src/backend/access/hash/hashfunc.c: 217 - 231

## Overview
Computes a 64-bit extended hash value for a float8 (double precision floating-point) value using a provided seed.

## Definition


## Detailed Description
This function is an extended version of the standard float8 hash function that accepts an additional seed parameter for hash computation. It handles special floating-point cases:
- Zero values (both positive and negative zero) are normalized to return the seed value directly
- NaN (Not a Number) values are normalized using  to ensure consistent hashing
- Uses the generic  function to compute the final hash with the provided seed

## Parameters / Member Variables
- : The float8 value to be hashed
- : The 64-bit seed value for hash computation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extract float8 argument from function call
  - PG_GETARG_INT64: Extract int64 seed argument from function call
  - PG_RETURN_UINT64: Return 64-bit unsigned integer result
  - isnan: Check if the float value is NaN
  - get_float8_nan: Get normalized NaN representation
  - hash_any_extended: Generic extended hash function for binary data
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Part of PostgreSQL's hash index infrastructure for float8 data types
- Ensures consistent hash values for special floating-point cases (±0, NaN)
- The extended version allows for hash table implementations that require seeded hashing
- Located in src/backend/access/hash/hashfunc.c:217-231