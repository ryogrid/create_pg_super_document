# int4eqfast

## Location
[src/backend/utils/cache/catcache.c:232-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L232-L237)

## Overview
A fast equality comparison function for 32-bit integers used in PostgreSQL's catalog cache system to quickly compare INT4OID type keys.

## Definition

```c
static bool
int4eqfast(Datum a, Datum b)
```
## Detailed Description
int4eqfast is a specialized equality function designed for high-performance comparison of 32-bit integer values in the catalog cache system. It provides a fast path for comparing INT4 (32-bit integer) values by directly extracting the integer values from Datum objects and performing a simple equality comparison. This function is part of PostgreSQL's catalog cache optimization, where fast lookup functions are essential for system performance.

The function is used specifically by the catalog cache system (catcache) to efficiently compare cache keys of INT4 type, avoiding the overhead of the full PostgreSQL function call mechanism when possible.

## Parameters / Member Variables
- `a`: First Datum containing a 32-bit integer value to compare
- `b`: Second Datum containing a 32-bit integer value to compare
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md) (macro for extracting int32 from Datum)
- Called from (representative examples):
  - [GetCCHashEqFuncs](../G/GetCCHashEqFuncs.md) (assigned as fast equality function for INT4OID and various REG* types)

## Notes and Other Information
- This function is static and only used within catcache.c
- Used for multiple PostgreSQL types: INT4OID and all REG* types (REGPROCOID, REGPROCEDUREOID, REGOPEROID, etc.) since they are all internally represented as 32-bit integers
- Part of the catalog cache optimization system that provides fast lookup functions for commonly used data types
- The function bypasses the normal PostgreSQL function call overhead for better performance in cache operations
- Returns a simple boolean result indicating whether the two integer values are equal

## Simplified Source

```c
static bool int4eqfast(Datum a, Datum b) {
    // Fast equality check: extract int32 values from Datums and compare
    return DatumGetInt32(a) == DatumGetInt32(b);
}
```