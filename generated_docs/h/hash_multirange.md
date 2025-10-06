# hash_multirange

## Location
[src/backend/utils/adt/multirangetypes.c:2787-2857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2787-L2857)

## Overview
Computes a 32-bit hash value for a multirange data type, which is used for hash-based operations like hash joins and hash indexes.

## Definition

```c
Datum
hash_multirange(PG_FUNCTION_ARGS)
```
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
  - [hash_uint32](hash_uint32.md) - [Hash](../H/Hash.md) the range flags
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

## Simplified Source

```c
Datum
hash_multirange(PG_FUNCTION_ARGS)
{
    MultirangeType *multirange = PG_GETARG_MULTIRANGE_P(0);
    uint32 result = 1;
    TypeCacheEntry *typcache, *element_cache;
    int32 range_count, i;

    // Get type cache and validate hash function availability
    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(multirange));
    element_cache = typcache->rngtype->rngelemtype;

    // Error handling for missing hash function
    if (!OidIsValid(element_cache->hash_proc_finfo.fn_oid)) {
        element_cache = lookup_type_cache(element_cache->type_id, TYPECACHE_HASH_PROC_FINFO);
        // Report error if still no hash function available
    }

    // Hash each range in the multirange
    range_count = multirange->rangeCount;
    for (i = 0; i < range_count; i++) {
        RangeBound lower, upper;
        uint8 flags = MultirangeGetFlagsPtr(multirange)[i];
        uint32 lower_hash, upper_hash, range_hash;

        // Extract bounds for this range
        multirange_get_bounds(typcache->rngtype, multirange, i, &lower, &upper);

        // Hash lower bound if present
        if (RANGE_HAS_LBOUND(flags))
            lower_hash = DatumGetUInt32(FunctionCall1Coll(&element_cache->hash_proc_finfo,
                                                         typcache->rngtype->rng_collation,
                                                         lower.val));
        else
            lower_hash = 0;

        // Hash upper bound if present
        if (RANGE_HAS_UBOUND(flags))
            upper_hash = DatumGetUInt32(FunctionCall1Coll(&element_cache->hash_proc_finfo,
                                                         typcache->rngtype->rng_collation,
                                                         upper.val));
        else
            upper_hash = 0;

        // Combine hashes: flags + lower + upper with rotation for distribution
        range_hash = hash_uint32((uint32) flags);
        range_hash ^= lower_hash;
        range_hash = pg_rotate_left32(range_hash, 1);
        range_hash ^= upper_hash;

        // Accumulate into final result using hash_array approach
        result = (result << 5) - result + range_hash;
    }

    PG_FREE_IF_COPY(multirange, 0);
    PG_RETURN_UINT32(result);
}
```