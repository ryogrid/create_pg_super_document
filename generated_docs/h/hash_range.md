# hash_range

## Location
[src/backend/utils/adt/rangetypes.c:1330-1395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1330-L1395)

## Overview
PostgreSQL function that computes a hash value for range data types, enabling range types to be used in hash-based operations like hash joins and hash indexes.

## Definition
```c
Datum hash_range(PG_FUNCTION_ARGS)
```

## Detailed Description
The `hash_range` function implements hash support for PostgreSQL range data types by computing a 32-bit hash value that represents the range. The function deserializes the range into its component parts (lower bound, upper bound, and flags), then applies the element type's hash function to each bound value. The final hash is computed by combining the hashed flag values and bound values using XOR operations and bit rotation to ensure good hash distribution.

The function handles empty ranges and ranges with missing bounds appropriately, using zero hash values for absent bounds. It also performs recursive checks for cases where the range's subtype is itself a range type, preventing stack overflow in deeply nested range types.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the range argument to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P (extracts range from function arguments)
  - RangeBound (structure representing range boundaries)  
  - check_stack_depth (prevents stack overflow in recursive cases)
  - [range_get_typcache](../r/range_get_typcache.md) (retrieves type cache information)
  - RangeTypeGetOid (gets range type OID)
  - [range_deserialize](../r/range_deserialize.md) (deserializes range into components)
  - [range_get_flags](../r/range_get_flags.md) (extracts range flags)
  - [lookup_type_cache](../l/lookup_type_cache.md) (looks up type cache with hash function info)
  - RANGE_HAS_LBOUND/RANGE_HAS_UBOUND (macros to check bound existence)
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md) (calls element type's hash function)
  - [DatumGetUInt32](../D/DatumGetUInt32.md) (extracts 32-bit result)
  - [hash_uint32](hash_uint32.md) (hashes the flags)
  - [pg_rotate_left32](../p/pg_rotate_left32.md) (rotates bits for better distribution)
- Called from (representative examples):
  - No direct references found in the codebase (likely used through hash operator dispatch)

## Notes and Other Information
- Essential for hash-based operations on range types including hash joins, hash aggregation, and hash indexes
- Ensures that equal ranges produce identical hash values while attempting to minimize collisions between different ranges
- Handles edge cases like empty ranges and ranges with infinite or missing bounds
- Uses the element type's own hash function to hash individual bound values, ensuring consistency with the element type's hash behavior
- Implements a robust hash combination strategy using XOR and bit rotation to mix the component hash values effectively
- Validates that the element type has a hash function available, throwing an error if none exists