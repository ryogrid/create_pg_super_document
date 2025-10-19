# hashenumextended

## Location
[src/backend/access/hash/hashfunc.c:134-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L134-L139)

## Overview
An extended PostgreSQL hash function for enumeration type values that supports a seed value for additional hash customization.

## Definition
```c
Datum hashenumextended(PG_FUNCTION_ARGS)
```

## Detailed Description
The `hashenumextended` function is the extended version of the `hashenum` hash function for PostgreSQL enumeration types. It takes two arguments: the enumeration value (as an OID) and a 64-bit seed value. The function extracts the enumeration value using `PG_GETARG_OID(0)`, casts it to a 32-bit unsigned integer, and passes it along with the seed value (obtained via `PG_GETARG_INT64(1)`) to the `hash_uint32_extended` function. This extended version allows for hash customization through the seed parameter, which is useful for hash partitioning and distributed hash operations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [hash_uint32_extended](hash_uint32_extended.md)
  - PG_GETARG_INT64
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is the extended version of `hashenum` that supports seeded hashing
- The seed parameter allows for hash distribution control and is commonly used in partitioned table operations
- Located in src/backend/access/hash/hashfunc.c at lines 134-139
- Follows PostgreSQL's pattern of providing both standard and extended versions of hash functions

## Simplified Source

```c
Datum hashenumextended(PG_FUNCTION_ARGS) {
    // Get enum OID value and seed, then hash using extended 32-bit hash function
    uint32 enum_value = (uint32) PG_GETARG_OID(0);
    int64 seed = PG_GETARG_INT64(1);
    return hash_uint32_extended(enum_value, seed);
}
```