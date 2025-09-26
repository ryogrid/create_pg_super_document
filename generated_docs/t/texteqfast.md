# texteqfast

## Location
[src/backend/utils/cache/catcache.c:244-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L244-L253)

## Overview
A fast equality comparison function for text values used in PostgreSQL's catalog cache system to compare TEXTOID type keys with deterministic collation.

## Definition
```c
static bool texteqfast(Datum a, Datum b)
```

## Detailed Description
texteqfast is a specialized equality function designed for comparing text values in the catalog cache system. It performs text comparison by calling the standard texteq function with DEFAULT_COLLATION_OID, ensuring a deterministic comparison path. The function is optimized for catalog cache usage where consistent and fast text comparison is essential.

Unlike direct string comparison, this function properly handles PostgreSQL's text type semantics and collation rules, using the default collation to ensure deterministic results. The comment in the code indicates that the use of DEFAULT_COLLATION_OID is deliberate to take the "deterministic" path in texteq(), avoiding locale-specific comparison complexities that could affect cache consistency.

## Parameters / Member Variables
- `a`: First Datum containing a text value to compare
- `b`: Second Datum containing a text value to compare

## Dependencies
- Functions called/Symbols referenced:
  - texteq (PostgreSQL's standard text equality function)
  - DirectFunctionCall2Coll (direct function call with collation)
- Called from (representative examples):
  - GetCCHashEqFuncs (assigned as fast equality function for TEXTOID)

## Notes and Other Information
- This function is static and only used within catcache.c
- Uses DEFAULT_COLLATION_OID to ensure deterministic comparison behavior
- The choice of default collation is described as "fairly arbitrary" in the code comments, with the primary goal being to use the deterministic path
- Part of the catalog cache optimization system for text-based cache keys
- Wraps the result of texteq in DatumGetBool for proper return type handling
- Ensures consistent text comparison behavior across different locales and collation settings
- Critical for maintaining cache correctness when text values are used as cache keys