# oidvectorhashfast

## Location
[src/backend/utils/cache/catcache.c:267-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L267-L273)

## Overview
A static hash function that computes a hash value for an oidvector datum, used internally by the catalog cache system for efficient indexing and lookup of oidvector values.

## Definition

```c
static uint32
oidvectorhashfast(Datum datum)
```
## Detailed Description
The  function is a simple wrapper around the standard  function that provides a fast hash computation for oidvector data types. It takes a Datum containing an oidvector and returns a 32-bit hash value. This function is specifically designed for use within the catalog cache system where fast hash computation is essential for performance. The function leverages PostgreSQL's function call infrastructure through  to invoke the standard oidvector hash function and converts the result to a proper uint32 hash value.

## Parameters / Member Variables
- `datum`: A Datum containing the oidvector value to be hashed
## Dependencies
- Functions called/Symbols referenced:
  - : The standard PostgreSQL hash function for oidvector types
  - : PostgreSQL's direct function call mechanism
  - : Utility function to extract int32 value from a Datum
- Called from (representative examples):
  - : Used as a hash function provider for catalog cache operations

## Notes and Other Information
- This is a static function, meaning it's only accessible within the catcache.c file
- The function serves as an optimization layer for catalog cache hash operations
- It specifically handles oidvector data types, which are commonly used in PostgreSQL's system catalogs
- The 'fast' suffix indicates this is an optimized version designed for frequent catalog cache operations

## Simplified Source

```c
static uint32 oidvectorhashfast(Datum datum) {
    // Fast oidvector hash: call standard hashoidvector function directly
    return DatumGetInt32(DirectFunctionCall1(hashoidvector, datum));
}
```