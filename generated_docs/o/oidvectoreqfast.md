# oidvectoreqfast

## Location
[src/backend/utils/cache/catcache.c:261-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L261-L266)

## Overview
A fast equality comparison function for OID vectors used in PostgreSQL's catalog cache system to compare OIDVECTOROID type keys.

## Definition
```c
static bool oidvectoreqfast(Datum a, Datum b)
```

## Detailed Description
oidvectoreqfast is a specialized equality function designed for comparing OID vector values in the catalog cache system. It performs comparison by calling the standard oidvectoreq function through the direct function call mechanism. OID vectors are commonly used in PostgreSQL's system catalogs to represent arrays of object identifiers, such as function parameter types or operator argument types.

This function provides a fast path for comparing oidvector values in the catalog cache, avoiding the overhead of the full PostgreSQL function resolution process. OID vectors are fixed-length arrays of OIDs (Object Identifiers) and are frequently used as cache keys in system catalog lookups.

## Parameters / Member Variables
- `a`: First Datum containing an oidvector value to compare
- `b`: Second Datum containing an oidvector value to compare

## Dependencies
- Functions called/Symbols referenced:
  - [oidvectoreq](oidvectoreq.md) (PostgreSQL's standard oidvector equality function)
  - DirectFunctionCall2 (direct function call without collation)
- Called from (representative examples):
  - [GetCCHashEqFuncs](../G/GetCCHashEqFuncs.md) (assigned as fast equality function for OIDVECTOROID)

## Notes and Other Information
- This function is static and only used within catcache.c
- Used specifically for OIDVECTOROID type in the catalog cache system
- OID vectors are commonly used to represent function signatures, operator argument lists, and similar system catalog metadata
- Part of the catalog cache optimization system for oidvector-based cache keys
- Wraps the result of oidvectoreq in DatumGetBool for proper return type handling
- Unlike text comparison functions, does not require collation parameters since OID comparison is inherently deterministic
- Essential for efficient lookup of functions, operators, and other database objects that use oidvector keys
- The direct function call approach bypasses PostgreSQL's function manager overhead for better cache performance

## Simplified Source

```c
static bool oidvectoreqfast(Datum a, Datum b) {
    // Fast oidvector equality: call standard oidvectoreq function directly
    return DatumGetBool(DirectFunctionCall2(oidvectoreq, a, b));
}
```