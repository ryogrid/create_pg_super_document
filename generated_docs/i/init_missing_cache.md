# init_missing_cache

## Location
[src/backend/access/common/heaptuple.c:122-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L122-L146)

## Overview
The `init_missing_cache` function initializes a hash table that caches missing attribute values for efficient lookup during PostgreSQL heap tuple processing operations.

## Definition
```c
static void init_missing_cache()
```

## Detailed Description
This function creates and configures a hash table specifically designed to cache missing attribute values in PostgreSQL. It sets up a hash table with custom hash and comparison functions (`missing_hash` and `missing_match`) and allocates it in the TopMemoryContext for persistent storage. The cache is named "Missing Values Cache" and is initialized with 32 buckets. The function configures the hash table to use element-based allocation, custom context, hash function, and comparison function.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - `[HASHCTL](../H/HASHCTL.md)`: Structure type for hash table control parameters
  - `missing_cache_key`: Structure type used for cache keys and entries
  - [missing_hash](../m/missing_hash.md): Custom hash function for cache keys
  - [missing_match](../m/missing_match.md): Custom comparison function for cache keys
  - [hash_create](../h/hash_create.md): PostgreSQL function to create hash tables
  - `TopMemoryContext`: Long-lived memory context for persistent storage
  - [Hash](../H/Hash.md) table flags: `HASH_ELEM`, `HASH_CONTEXT`, `HASH_FUNCTION`, `HASH_COMPARE`
- Called from (representative examples):
  - [getmissingattr](../g/getmissingattr.md): Calls this function to initialize the cache when first needed

## Notes and Other Information
- This is a static function with internal linkage within heaptuple.c
- The function is called lazily - only when the missing attribute cache is first needed
- Uses TopMemoryContext to ensure the cache persists across transactions
- The hash table starts with 32 buckets and can grow as needed
- Part of PostgreSQL optimization for handling missing attributes in tuple processing
- Sets global variable `missing_cache` to point to the created hash table

## Simplified Source

```c
static void init_missing_cache() {
    HASHCTL hash_ctl;

    // Configure hash table parameters
    hash_ctl.keysize = sizeof(missing_cache_key);
    hash_ctl.entrysize = sizeof(missing_cache_key);
    hash_ctl.hcxt = TopMemoryContext;  // Use persistent memory context
    hash_ctl.hash = missing_hash;      // Custom hash function
    hash_ctl.match = missing_match;    // Custom comparison function

    // Create the hash table for caching missing attribute values
    missing_cache = hash_create("Missing Values Cache",
                               32,  // Initial bucket count
                               &hash_ctl,
                               HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);
}
```