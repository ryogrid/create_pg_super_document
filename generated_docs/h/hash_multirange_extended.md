# hash_multirange_extended

## Location
src/backend/utils/adt/multirangetypes.c: 2858 - 2926

## Overview
Computes a 64-bit hash value for a multirange data type with an additional seed parameter, providing enhanced hash distribution for advanced hashing scenarios.

## Definition


## Detailed Description
This function is the extended version of hash_multirange that produces a 64-bit hash value instead of 32-bit, and accepts a seed parameter for hash randomization. It follows the same algorithmic approach as hash_multirange but uses extended hash functions for the element types and operates with 64-bit arithmetic throughout.

The function processes each range in the multirange by:
1. Extracting range bounds and flags for each constituent range
2. Computing 64-bit hash values for bounds using the element type's extended hash function with the provided seed
3. Computing a 64-bit hash for the range flags using hash_uint32_extended with the seed
4. Combining bound hashes with flag hash using XOR and 64-bit rotation
5. Accumulating all range hashes using the same approach as hash_array

The extended version provides better hash distribution and collision resistance, particularly useful for hash-based partitioning and distributed systems.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention providing:
  - Argument 0:  - The multirange value to hash
  - Argument 1:  - Seed value for hash randomization

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P - Extract multirange argument
  - PG_GETARG_DATUM - Extract seed argument
  - multirange_get_typcache - Get type cache for multirange type
  - MultirangeTypeGetOid - Get OID of multirange type
  - lookup_type_cache - Look up type cache with extended hash info
  - multirange_get_bounds - Extract bounds from a specific range
  - FunctionCall2Coll - Call element extended hash function with collation and seed
  - hash_uint32_extended - Hash the range flags with seed
  - ROTATE_HIGH_AND_LOW_32BITS - Rotate 64-bit hash for better distribution
  - RANGE_HAS_LBOUND/RANGE_HAS_UBOUND - Check bound existence
  - DatumGetUInt64/DatumGetInt64 - Convert Datum to 64-bit integers
  - PG_RETURN_UINT64 - Return the computed 64-bit hash value
- Called from:
  - Used internally by PostgreSQL's extended hash-based operations (no direct references found)

## Notes and Other Information
- Returns a 64-bit hash value providing better collision resistance than the 32-bit version
- The seed parameter enables hash randomization, useful for preventing hash-flooding attacks
- Requires that element types have extended hash function support (hash_extended_proc_finfo)
- Uses TYPECACHE_HASH_EXTENDED_PROC_FINFO for type cache lookup instead of the basic hash version
- The ROTATE_HIGH_AND_LOW_32BITS macro provides 64-bit specific rotation for optimal hash distribution
- Maintains the same error handling and memory management patterns as the basic hash_multirange function
- Essential for hash partitioning and distributed query processing where 64-bit hash values provide better key distribution