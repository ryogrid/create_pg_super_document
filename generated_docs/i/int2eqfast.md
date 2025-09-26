# int2eqfast

## Location
[src/backend/utils/cache/catcache.c:220-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L220-L225)

## Overview
The `int2eqfast` function provides a fast equality comparison for PostgreSQL's `int2` (smallint) data type, used internally by the catalog cache system for efficient key matching in hash tables.

## Definition
```c
static bool int2eqfast(Datum a, Datum b)
```

## Detailed Description
This function performs optimized equality comparison between two PostgreSQL `int2` (16-bit integer) values within the catalog cache system. It extracts 16-bit integer values from both Datum parameters using `DatumGetInt16` and performs a direct integer comparison. This straightforward approach provides maximum efficiency for catalog cache operations involving smallint keys, avoiding function call overhead typical of general-purpose comparison functions.

## Parameters / Member Variables
- `a`: First Datum containing an int2 value to compare
- `b`: Second Datum containing an int2 value to compare

## Dependencies
- Functions called/Symbols referenced:
  - `[DatumGetInt16](../D/DatumGetInt16.md)`: Extracts int16 value from Datum (called twice)
- Called from (representative examples):
  - `[GetCCHashEqFuncs](../G/GetCCHashEqFuncs.md)`: Function that retrieves hash and equality functions for catalog cache

## Notes and Other Information
- Extremely simple and efficient implementation using direct integer comparison
- Optimized for catalog cache performance where int2 equality checks are frequent
- Part of PostgreSQL's internal catalog cache optimization infrastructure
- Static function scope restricts usage to catcache.c compilation unit
- Complements `int2hashfast` for complete int2-based hash table operations
- No overflow concerns due to direct 16-bit integer comparison