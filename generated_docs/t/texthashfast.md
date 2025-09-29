# texthashfast

## Location
[src/backend/utils/cache/catcache.c:254-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L254-L260)

## Overview
A fast hash function for text values used in PostgreSQL's catalog cache system to generate hash values for TEXTOID type keys with deterministic collation.

## Definition
```c
static uint32 texthashfast(Datum datum)
```

## Detailed Description
texthashfast is a specialized hash function designed for generating hash values for text data in the catalog cache system. It computes hash values by calling the standard hashtext function with DEFAULT_COLLATION_OID, ensuring deterministic hashing behavior. The function is optimized for catalog cache usage where consistent hash distribution and deterministic results are critical for cache performance.

The function follows the same collation strategy as texteqfast, using DEFAULT_COLLATION_OID to ensure that hash values are computed consistently regardless of the session's locale settings. This deterministic approach is essential for cache correctness, as the same text value must always produce the same hash value across different sessions and locales.

## Parameters / Member Variables
- `datum`: Datum containing a text value to hash

## Dependencies
- Functions called/Symbols referenced:
  - [hashtext](../h/hashtext.md) (PostgreSQL's standard text hashing function)
  - [DirectFunctionCall1Coll](../D/DirectFunctionCall1Coll.md) (direct function call with collation)
  - [DatumGetInt32](../D/DatumGetInt32.md) (macro for extracting int32 result from Datum)
- Called from (representative examples):
  - [GetCCHashEqFuncs](../G/GetCCHashEqFuncs.md) (assigned as hash function for TEXTOID)

## Notes and Other Information
- This function is static and only used within catcache.c
- Uses DEFAULT_COLLATION_OID to ensure deterministic hashing behavior, analogous to texteqfast()
- The code comment references the same design rationale as texteqfast for collation choice
- Part of the catalog cache optimization system for text-based cache keys
- Returns a 32-bit hash value suitable for hash table indexing
- Critical for maintaining cache performance and correctness when text values are used as cache keys
- Ensures that equivalent text values (as determined by texteqfast) produce identical hash values
- The deterministic collation approach prevents hash value variations across different locale settings

## Simplified Source

```c
static uint32 texthashfast(Datum datum) {
    // Use default collation for deterministic hashing
    return DatumGetInt32(DirectFunctionCall1Coll(hashtext, DEFAULT_COLLATION_OID, datum));
}
```