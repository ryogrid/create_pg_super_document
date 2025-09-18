# hash_range_extended

## Location
[src/backend/utils/adt/rangetypes.c:1396-1463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1396-L1463)

## Overview
Computes a 64-bit hash value for a range type with an extended hashing algorithm that includes a seed value, providing better hash distribution for range types in hash-based operations.

## Definition


## Detailed Description
This function is the extended version of the range hash function that produces a 64-bit hash value with a seed parameter. It deserializes the input range, extracts the lower and upper bounds, and computes hash values for each bound using the element type's extended hash function. The final hash is computed by combining the hashes of the range flags, lower bound, and upper bound through XOR operations and bit rotation to ensure good distribution.

The function handles empty ranges and ranges with missing bounds appropriately, using zero hash values for missing bounds. It requires that the range's element type has an extended hash function available.

## Parameters / Member Variables
- : The input range value to be hashed (accessed via )
- : The seed value for the hash function (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - PG_GETARG_DATUM  
  - check_stack_depth
  - [range_get_typcache](../r/range_get_typcache.md)
  - RangeTypeGetOid
  - [range_deserialize](../r/range_deserialize.md)
  - [range_get_flags](../r/range_get_flags.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [hash_uint32_extended](hash_uint32_extended.md)
  - ROTATE_HIGH_AND_LOW_32BITS
  - PG_RETURN_UINT64
- Called from (representative examples):
  - No direct references found (likely called via function catalog)

## Notes and Other Information
- This is the extended version of hash_range that provides 64-bit output and accepts a seed parameter
- The function performs stack depth checking to prevent stack overflow in recursive scenarios
- Error handling ensures that the element type has an extended hash function available
- Uses bit rotation and XOR operations to combine hash values for better distribution
- The hash combines range flags, lower bound hash, and upper bound hash in a specific pattern