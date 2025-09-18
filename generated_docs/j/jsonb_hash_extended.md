# jsonb_hash_extended

## Location
src/backend/utils/adt/jsonb_op.c: 295 - 335

## Overview
Computes an extended 64-bit hash value for a JSONB value using a seed value, providing enhanced hash distribution for advanced hashing applications.

## Definition
Datum jsonb_hash_extended(PG_FUNCTION_ARGS)

## Detailed Description
The jsonb_hash_extended function is an enhanced version of jsonb_hash that generates a 64-bit hash value using a seed parameter for improved hash distribution. This extended hashing function is typically used in advanced hash-based operations that require better hash quality, such as hash partitioning or sophisticated hash join algorithms.

Similar to jsonb_hash, it iterates through the JSONB structure using JsonbIterator, but with key differences:
- Uses 64-bit hash values instead of 32-bit
- Accepts a seed parameter to influence hash computation
- Returns the seed value directly for empty JSONB structures
- Uses JsonbHashScalarValueExtended for scalar value hashing
- Applies 64-bit XOR operations for structural elements, combining high and low 32-bit portions

The function maintains the same structural iteration pattern as jsonb_hash but provides enhanced collision resistance and distribution properties through the extended hash space and seeded computation.

## Parameters / Member Variables
- : JSONB value to hash (jb)
- : 64-bit seed value for hash computation (seed)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - PG_GETARG_INT64
  - JB_ROOT_COUNT
  - PG_RETURN_UINT64
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - [JsonbHashScalarValueExtended](../J/JsonbHashScalarValueExtended.md) (src/backend/utils/adt/jsonb_util.c:1365-1406)
  - PG_FREE_IF_COPY
  - elog
- Data types used:
  - Jsonb
  - JsonbIterator
  - [JsonbValue](../J/JsonbValue.md)
  - JsonbIteratorToken
  - uint64
- Constants used:
  - WJB_DONE, WJB_BEGIN_ARRAY, WJB_BEGIN_OBJECT, WJB_KEY, WJB_VALUE, WJB_ELEM, WJB_END_ARRAY, WJB_END_OBJECT
  - JB_FARRAY, JB_FOBJECT

## Notes and Other Information
- Location: src/backend/utils/adt/jsonb_op.c:295-335
- Provides enhanced hash quality compared to the standard jsonb_hash function
- Uses 64-bit hash computation for better collision resistance
- Seed parameter allows for hash randomization and improved distribution
- For empty JSONB values, returns the seed value directly as an optimization
- Structural elements use 64-bit XOR with duplicated 32-bit patterns for consistent bit distribution
- Essential for advanced hash-based operations requiring higher-quality hash functions
- Maintains compatibility with JSONB iterator patterns while providing extended functionality