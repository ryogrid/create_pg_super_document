# uuid_hash_extended

## Location
[src/backend/utils/adt/uuid.c:403-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L403-L410)

## Overview
An extended hash function for UUIDs that incorporates a seed value to support advanced hashing scenarios like hash partitioning and salted hash operations.

## Definition
```c
Datum uuid_hash_extended(PG_FUNCTION_ARGS)  
```

## Detailed Description
This function provides extended hash support for the UUID data type by computing a seeded hash value over the UUID data. It extends the basic `uuid_hash` functionality by accepting an additional 64-bit seed parameter, enabling use cases that require hash value variation based on context, such as hash partitioning, distributed systems coordination, and cryptographic applications.

The function leverages PostgreSQL's `hash_any_extended` function to compute the seeded hash, ensuring that the same UUID will produce different hash values when combined with different seeds. This capability is essential for certain advanced database operations and distributed query processing scenarios.

## Parameters / Member Variables  
- Expects two arguments:
  1. The UUID value to hash
  2. A 64-bit integer seed value

## Dependencies
- Functions called/Symbols referenced:
  - [pg_uuid_t](../p/pg_uuid_t.md) (UUID structure type)
  - PG_GETARG_UUID_P (UUID argument extraction macro)
  - [hash_any_extended](../h/hash_any_extended.md) (extended hash function with seed)
  - UUID_LEN (UUID length constant - 16 bytes)
  - PG_GETARG_INT64 (64-bit integer argument extraction macro)
- Called from (representative examples):
  - [Hash](../H/Hash.md) partitioning operations
  - Distributed query processing
  - Advanced hash-based algorithms

## Notes and Other Information
- Provides seeded hashing capability for scenarios requiring hash variation
- Essential for hash partitioning where different partition schemes need different hash functions
- Uses the same underlying hash algorithm as the basic version but with seed support
- The 64-bit seed allows for a very large number of distinct hash variations
- Maintains the same distribution properties as the base hash function while adding seed-based variation
- Commonly used in parallel and distributed database operations

## Simplified Source

```c
Datum uuid_hash_extended(PG_FUNCTION_ARGS) {
    // Extract UUID and seed from function arguments
    pg_uuid_t *key = PG_GETARG_UUID_P(0);
    int64 seed = PG_GETARG_INT64(1);

    // Hash all 16 bytes of UUID data with the provided seed
    return hash_any_extended(key->data, UUID_LEN, seed);
}
```