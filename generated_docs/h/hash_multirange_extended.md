# hash_multirange_extended

## Location
[src/backend/utils/adt/multirangetypes.c:2858-2926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L2858-L2926)

## Overview
Computes a 64-bit hash value for a multirange data type with an additional seed parameter, providing enhanced hash distribution for advanced hashing scenarios.

## Definition

```c
Datum
hash_multirange_extended(PG_FUNCTION_ARGS)
```
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
  - [multirange_get_typcache](../m/multirange_get_typcache.md) - Get type cache for multirange type
  - MultirangeTypeGetOid - Get OID of multirange type
  - [lookup_type_cache](../l/lookup_type_cache.md) - Look up type cache with extended hash info
  - [multirange_get_bounds](../m/multirange_get_bounds.md) - Extract bounds from a specific range
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) - Call element extended hash function with collation and seed
  - [hash_uint32_extended](hash_uint32_extended.md) - [Hash](../H/Hash.md) the range flags with seed
  - ROTATE_HIGH_AND_LOW_32BITS - Rotate 64-bit hash for better distribution
  - RANGE_HAS_LBOUND/RANGE_HAS_UBOUND - Check bound existence
  - [DatumGetUInt64](../D/DatumGetUInt64.md)/DatumGetInt64 - Convert Datum to 64-bit integers
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

## Simplified Source

```c
Datum
hash_multirange_extended(PG_FUNCTION_ARGS)
{
    MultirangeType *multirange = PG_GETARG_MULTIRANGE_P(0);
    Datum seed = PG_GETARG_DATUM(1);
    uint64 result = 1;
    TypeCacheEntry *typcache, *element_cache;
    int32 range_count, i;

    // Get type cache and validate extended hash function availability
    typcache = multirange_get_typcache(fcinfo, MultirangeTypeGetOid(multirange));
    element_cache = typcache->rngtype->rngelemtype;

    // Error handling for missing extended hash function
    if (!OidIsValid(element_cache->hash_extended_proc_finfo.fn_oid)) {
        element_cache = lookup_type_cache(element_cache->type_id, TYPECACHE_HASH_EXTENDED_PROC_FINFO);
        // Report error if still no extended hash function available
    }

    // Hash each range in the multirange using 64-bit arithmetic
    range_count = multirange->rangeCount;
    for (i = 0; i < range_count; i++) {
        RangeBound lower, upper;
        uint8 flags = MultirangeGetFlagsPtr(multirange)[i];
        uint64 lower_hash, upper_hash, range_hash;

        // Extract bounds for this range
        multirange_get_bounds(typcache->rngtype, multirange, i, &lower, &upper);

        // Hash lower bound with seed if present
        if (RANGE_HAS_LBOUND(flags))
            lower_hash = DatumGetUInt64(FunctionCall2Coll(&element_cache->hash_extended_proc_finfo,
                                                         typcache->rngtype->rng_collation,
                                                         lower.val, seed));
        else
            lower_hash = 0;

        // Hash upper bound with seed if present
        if (RANGE_HAS_UBOUND(flags))
            upper_hash = DatumGetUInt64(FunctionCall2Coll(&element_cache->hash_extended_proc_finfo,
                                                         typcache->rngtype->rng_collation,
                                                         upper.val, seed));
        else
            upper_hash = 0;

        // Combine 64-bit hashes: flags + lower + upper with 64-bit rotation
        range_hash = DatumGetUInt64(hash_uint32_extended((uint32) flags, DatumGetInt64(seed)));
        range_hash ^= lower_hash;
        range_hash = ROTATE_HIGH_AND_LOW_32BITS(range_hash);
        range_hash ^= upper_hash;

        // Accumulate into final 64-bit result using hash_array approach
        result = (result << 5) - result + range_hash;
    }

    PG_FREE_IF_COPY(multirange, 0);
    PG_RETURN_UINT64(result);
}
```