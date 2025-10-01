# get_func_namespace

## Location
[src/backend/utils/cache/lsyscache.c:1632-1654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1632-L1654)

## Overview
Returns the pg_namespace OID associated with a given function, providing namespace information for function identification.

## Definition

```c
Oid
get_func_namespace(Oid funcid)
```
## Detailed Description
This function retrieves the namespace (schema) OID for a specified function by looking up the function in the system catalog. It performs a system cache lookup on the pg_proc table using the function's OID and extracts the pronamespace field which contains the namespace OID. The function returns InvalidOid if the function does not exist in the catalog.

## Parameters / Member Variables
- : The OID of the function whose namespace is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): Searches the system cache for the function entry
  - HeapTupleIsValid: Validates the returned heap tuple
  - GETSTRUCT: Extracts the struct from the heap tuple
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases the system cache entry
  - Form_pg_proc: PostgreSQL system catalog structure for procedures/functions
- Called from (representative examples):
  - [ExplainTargetRel](../E/ExplainTargetRel.md): Used in query explanation functionality

## Notes and Other Information
- Part of PostgreSQL's system catalog lookup utilities in lsyscache.c
- Uses system cache for efficient repeated lookups
- Returns InvalidOid for non-existent functions rather than throwing an error
- The namespace OID can be used to determine the schema name via additional catalog lookups

## Simplified Source

```c
Oid
get_func_namespace(Oid funcid)
{
    HeapTuple tp;

    // Look up function in system cache
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));

    if (HeapTupleIsValid(tp))
    {
        // Extract namespace from function tuple
        Form_pg_proc functup = (Form_pg_proc) GETSTRUCT(tp);
        Oid result = functup->pronamespace;

        ReleaseSysCache(tp);
        return result;
    }
    else
        return InvalidOid; // Function not found
}
```