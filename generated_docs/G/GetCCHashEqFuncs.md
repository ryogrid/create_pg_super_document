# GetCCHashEqFuncs

## Location
[src/backend/utils/cache/catcache.c:274-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L274-L343)

## Overview
A static function that determines the appropriate hash function, equality function, and fast equality function for a given PostgreSQL data type, used by the catalog cache system to optimize key comparisons and hash computations.

## Definition

```c
static void
GetCCHashEqFuncs(Oid keytype, CCHashFN *hashfunc, RegProcedure *eqfunc, CCFastEqualFN *fasteqfunc)
```
## Detailed Description
The `GetCCHashEqFuncs` function serves as a central dispatcher that maps PostgreSQL data types to their corresponding optimized hash and equality functions for catalog cache operations. Given a type OID, it sets up three function pointers: a fast hash function, a fast equality function, and a standard equality procedure. The function uses a large switch statement to handle various built-in PostgreSQL types including booleans, characters, names, integers, text, OIDs, and OID vectors. For unsupported types, it raises a FATAL error since the catalog cache system requires these functions to operate correctly. This function is crucial for catalog cache performance as it enables type-specific optimizations for the most commonly used system catalog key types.

## Parameters / Member Variables
- `keytype`: The OID of the PostgreSQL data type for which functions are needed
- `hashfunc`: Output parameter that receives a pointer to the appropriate fast hash function
- `eqfunc`: Output parameter that receives the RegProcedure OID for the standard equality function
- `fasteqfunc`: Output parameter that receives a pointer to the optimized fast equality function

## Dependencies
- Functions called/Symbols referenced:
  - `[charhashfast](../c/charhashfast.md)`: Fast hash function for char/boolean types
  - `[chareqfast](../c/chareqfast.md)`: Fast equality function for char/boolean types
  - `[namehashfast](../n/namehashfast.md)`: Fast hash function for name type
  - `[nameeqfast](../n/nameeqfast.md)`: Fast equality function for name type
  - `[int2hashfast](../i/int2hashfast.md)`: Fast hash function for int2 type
  - `[int2eqfast](../i/int2eqfast.md)`: Fast equality function for int2 type
  - `[int4hashfast](../i/int4hashfast.md)`: Fast hash function for int4/OID types
  - `[int4eqfast](../i/int4eqfast.md)`: Fast equality function for int4/OID types
  - `[texthashfast](../t/texthashfast.md)`: Fast hash function for text type
  - `[texteqfast](../t/texteqfast.md)`: Fast equality function for text type
  - [oidvectorhashfast](../o/oidvectorhashfast.md): Fast hash function for oidvector type
  - `[oidvectoreqfast](../o/oidvectoreqfast.md)`: Fast equality function for oidvector type
- Called from (representative examples):
  - [CatalogCacheInitializeCache](../C/CatalogCacheInitializeCache.md): Used during catalog cache initialization to set up type-specific functions

## Notes and Other Information
- This function supports a comprehensive set of PostgreSQL built-in types commonly used as catalog keys
- The function handles multiple OID-related types (OIDOID, REGPROCOID, etc.) by mapping them all to int4 hash/equality functions
- Unsupported types result in a FATAL error, ensuring that catalog cache operations never proceed with invalid function pointers
- The `fast` versions of equality and hash functions are optimized specifically for catalog cache performance
- This function is static and only accessible within the catcache.c file

## Simplified Source

```c
static void
GetCCHashEqFuncs(Oid keytype, CCHashFN *hashfunc, RegProcedure *eqfunc, CCFastEqualFN *fasteqfunc)
{
    // Map PostgreSQL data types to their optimized hash and equality functions
    switch (keytype)
    {
        case BOOLOID:
        case CHAROID:
            // Character-based types use char hash/equality functions
            *hashfunc = charhashfast;
            *fasteqfunc = chareqfast;
            *eqfunc = (keytype == BOOLOID) ? F_BOOLEQ : F_CHAREQ;
            break;

        case NAMEOID:
            // Name type has specialized functions
            *hashfunc = namehashfast;
            *fasteqfunc = nameeqfast;
            *eqfunc = F_NAMEEQ;
            break;

        case INT2OID:
        case INT4OID:
            // Integer types use type-specific functions
            *hashfunc = (keytype == INT2OID) ? int2hashfast : int4hashfast;
            *fasteqfunc = (keytype == INT2OID) ? int2eqfast : int4eqfast;
            *eqfunc = (keytype == INT2OID) ? F_INT2EQ : F_INT4EQ;
            break;

        case TEXTOID:
            // Text type has specialized functions
            *hashfunc = texthashfast;
            *fasteqfunc = texteqfast;
            *eqfunc = F_TEXTEQ;
            break;

        case OIDOID:
        case REGPROCOID:
        case REGPROCEDUREOID:
        case REGOPEROID:
        case REGOPERATOROID:
        case REGCLASSOID:
        case REGTYPEOID:
        case REGCOLLATIONOID:
        case REGCONFIGOID:
        case REGDICTIONARYOID:
        case REGROLEOID:
        case REGNAMESPACEOID:
            // All OID-related types use int4 functions with OID equality
            *hashfunc = int4hashfast;
            *fasteqfunc = int4eqfast;
            *eqfunc = F_OIDEQ;
            break;

        case OIDVECTOROID:
            // OID vector has specialized functions
            *hashfunc = oidvectorhashfast;
            *fasteqfunc = oidvectoreqfast;
            *eqfunc = F_OIDVECTOREQ;
            break;

        default:
            // Unsupported types cause fatal error
            elog(FATAL, "type %u not supported as catcache key", keytype);
            break;
    }
}
```