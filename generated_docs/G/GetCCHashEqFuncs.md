# GetCCHashEqFuncs

## Location
src/backend/utils/cache/catcache.c: 274 - 343

## Overview
A static function that determines the appropriate hash function, equality function, and fast equality function for a given PostgreSQL data type, used by the catalog cache system to optimize key comparisons and hash computations.

## Definition


## Detailed Description
The `GetCCHashEqFuncs` function serves as a central dispatcher that maps PostgreSQL data types to their corresponding optimized hash and equality functions for catalog cache operations. Given a type OID, it sets up three function pointers: a fast hash function, a fast equality function, and a standard equality procedure. The function uses a large switch statement to handle various built-in PostgreSQL types including booleans, characters, names, integers, text, OIDs, and OID vectors. For unsupported types, it raises a FATAL error since the catalog cache system requires these functions to operate correctly. This function is crucial for catalog cache performance as it enables type-specific optimizations for the most commonly used system catalog key types.

## Parameters / Member Variables
- `keytype`: The OID of the PostgreSQL data type for which functions are needed
- `hashfunc`: Output parameter that receives a pointer to the appropriate fast hash function
- `eqfunc`: Output parameter that receives the RegProcedure OID for the standard equality function
- `fasteqfunc`: Output parameter that receives a pointer to the optimized fast equality function

## Dependencies
- Functions called/Symbols referenced:
  - `charhashfast`: Fast hash function for char/boolean types
  - `chareqfast`: Fast equality function for char/boolean types
  - `namehashfast`: Fast hash function for name type
  - `nameeqfast`: Fast equality function for name type
  - `int2hashfast`: Fast hash function for int2 type
  - `int2eqfast`: Fast equality function for int2 type
  - `int4hashfast`: Fast hash function for int4/OID types
  - `int4eqfast`: Fast equality function for int4/OID types
  - `texthashfast`: Fast hash function for text type
  - `texteqfast`: Fast equality function for text type
  - `[oidvectorhashfast](../o/oidvectorhashfast.md)`: Fast hash function for oidvector type
  - `oidvectoreqfast`: Fast equality function for oidvector type
- Called from (representative examples):
  - `[CatalogCacheInitializeCache](../C/CatalogCacheInitializeCache.md)`: Used during catalog cache initialization to set up type-specific functions

## Notes and Other Information
- This function supports a comprehensive set of PostgreSQL built-in types commonly used as catalog keys
- The function handles multiple OID-related types (OIDOID, REGPROCOID, etc.) by mapping them all to int4 hash/equality functions
- Unsupported types result in a FATAL error, ensuring that catalog cache operations never proceed with invalid function pointers
- The `fast` versions of equality and hash functions are optimized specifically for catalog cache performance
- This function is static and only accessible within the catcache.c file