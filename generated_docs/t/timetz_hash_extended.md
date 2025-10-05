# timetz_hash_extended

## Location
[src/backend/utils/adt/date.c:2549-2564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2549-L2564)

## Overview
The timetz_hash_extended function computes an extended 64-bit hash value for a TimeTzADT (time with time zone) value using an additional seed parameter for enhanced hash distribution.

## Definition
Datum timetz_hash_extended(PG_FUNCTION_ARGS)

## Detailed Description
This function implements extended hash computation for the TimeTzADT data type, providing a 64-bit hash value instead of the 32-bit hash from timetz_hash. The extended version takes an additional seed parameter that allows for better hash distribution and is particularly useful in advanced hashing scenarios like hash joins with multiple hash tables or when hash collision reduction is important.

Like its non-extended counterpart, this function computes separate hash values for each field of the TimeTzADT structure and combines them using XOR to avoid issues with struct padding bytes. However, it uses the extended versions of the underlying hash functions that accept a seed parameter and produce 64-bit results.

The extended hashing process:
1. Hashes the time component (int64) using hashint8extended with the provided seed
2. Hashes the zone component (int32) using hash_uint32_extended with the seed
3. XORs the two 64-bit hash values together to produce the final result

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function argument structure containing:
  - Argument 0: TimeTzADT value (key) to be hashed
  - Argument 1: Datum seed value for hash computation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P: Extracts TimeTzADT argument from function call
  - PG_GETARG_DATUM: Extracts seed Datum from function call
  - [hashint8extended](../h/hashint8extended.md): Computes extended hash value for 64-bit integer (time component)
  - [hash_uint32_extended](../h/hash_uint32_extended.md): Computes extended hash value for 32-bit unsigned integer (zone component)
  - DirectFunctionCall2: PostgreSQL function call mechanism for two-argument functions
  - [DatumGetUInt64](../D/DatumGetUInt64.md): Extracts uint64 value from Datum
  - [DatumGetInt64](../D/DatumGetInt64.md): Extracts int64 value from Datum
  - Int64GetDatumFast: Converts int64 to Datum efficiently
  - PG_RETURN_UINT64: Returns uint64 result to PostgreSQL function call framework
- Data types used:
  - TimeTzADT: Structure containing time (TimeADT) and zone (int32) fields
- Called from (representative examples):
  - Advanced hash table operations requiring better distribution
  - [Hash](../H/Hash.md) joins with multiple hash phases
  - Partitioned hash operations

## Notes and Other Information
- Provides enhanced hash distribution compared to the basic timetz_hash function
- Returns 64-bit hash values for better collision resistance in large datasets
- Uses the same XOR combination approach as timetz_hash to avoid struct padding issues
- The seed parameter allows for randomization and multi-level hashing scenarios
- Must maintain consistency with equality semantics defined by timetz_cmp
- Returns a PostgreSQL Datum containing an unsigned 64-bit integer hash value
- Located in src/backend/utils/adt/date.c:2549-2564
- Part of PostgreSQL's extended hashing infrastructure for improved performance in complex query scenarios

## Simplified Source

```c
Datum timetz_hash_extended(PG_FUNCTION_ARGS) {
    TimeTzADT *key = PG_GETARG_TIMETZADT_P(0);
    Datum seed = PG_GETARG_DATUM(1);

    // Hash time and zone components separately using extended hash functions with seed
    uint64 time_hash = DatumGetUInt64(DirectFunctionCall2(hashint8extended,
                                      Int64GetDatumFast(key->time), seed));
    uint64 zone_hash = DatumGetUInt64(hash_uint32_extended(key->zone,
                                      DatumGetInt64(seed)));

    // XOR the two 64-bit hash values together for final result
    uint64 combined_hash = time_hash ^ zone_hash;

    PG_RETURN_UINT64(combined_hash);
}
```