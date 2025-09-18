# hash_multirange

## Location
src/backend/utils/adt/multirangetypes.c: 2787 - 2857

## Overview
Computes a 32-bit hash value for a multirange data type, which is used for hash-based operations like hash joins and hash indexes.

## Definition


## Detailed Description
This function implements hash support for PostgreSQL's multirange data types by computing a hash value from all constituent ranges within the multirange. The algorithm iterates through each range in the multirange, hashes the lower and upper bounds of each range (if they exist), combines them with the range flags, and accumulates the results using a left-shift and XOR approach similar to hash_array.

The function first validates that hash functions are available for the element type of the range, then processes each range by:
1. Extracting the range bounds and flags
2. Computing hash values for non-null bounds using the element type's hash function
3. Combining the flag hash with bound hashes using XOR and rotation
4. Accumulating all range hashes using a multiplication-based approach

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention providing:
  - Argument 0:  - The multirange value to hash

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P - Extract multirange argument
  - [multirange_get_typcache](../m/multirange_get_typcache.md) - Get type cache for multirange type
  - MultirangeTypeGetOid - Get OID of multirange type
  - [lookup_type_cache](../l/lookup_type_cache.md) - Look up type cache information
  - [multirange_get_bounds](../m/multirange_get_bounds.md) - Extract bounds from a specific range
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md) - Call element hash function with collation
  - [hash_uint32](hash_uint32.md) - Hash the range flags
  - [pg_rotate_left32](../p/pg_rotate_left32.md) - Rotate hash bits for better distribution
  - RANGE_HAS_LBOUND/RANGE_HAS_UBOUND - Check bound existence
  - [DatumGetUInt32](../D/DatumGetUInt32.md) - Convert Datum to uint32
  - PG_RETURN_UINT32 - Return the computed hash value
- Called from: 
  - Used internally by PostgreSQL's hash-based operations (no direct references found)

## Notes and Other Information
- Returns a 32-bit hash value suitable for hash tables and hash-based joins
- The function ensures hash consistency by processing ranges in their stored order
- Uses the same hash combination technique as hash_array for predictable collision behavior
- Validates availability of hash functions for element types and reports errors if unavailable
- Memory cleanup is performed using PG_FREE_IF_COPY to handle toasted values
- The hash algorithm combines range flags and bounds to ensure different multiranges with similar bounds but different inclusivity produce different hash values