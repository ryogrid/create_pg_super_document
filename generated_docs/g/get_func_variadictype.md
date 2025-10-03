# get_func_variadictype

## Location
[src/backend/utils/cache/lsyscache.c:1723-1741](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1723-L1741)

## Overview
Returns the variadic argument type OID for a given function, indicating what type the function's variadic parameters accept.

## Definition

```c
Oid
get_func_variadictype(Oid funcid)
```
## Detailed Description
This function retrieves the variadic type information for a specified function by performing a system cache lookup on the pg_proc table. It extracts the provariadic field from the function's catalog entry, which contains the OID of the type that the function's variadic parameters accept. If the function is not variadic, this field will be InvalidOid (0). The function is used to determine how to handle variable-length argument lists in function calls.

## Parameters / Member Variables
- `funcid`: The OID of the function whose variadic type is to be retrieved
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): Searches the system cache for the function entry
  - HeapTupleIsValid: Validates the returned heap tuple
  - elog: Logs error if function not found
  - GETSTRUCT: Extracts the struct from the heap tuple
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases the system cache entry
  - Form_pg_proc: PostgreSQL system catalog structure for procedures/functions
- Called from (representative examples):
  - [assign_ordered_set_collations](../a/assign_ordered_set_collations.md): Used in collation assignment for ordered set aggregates
  - [assign_hypothetical_collations](../a/assign_hypothetical_collations.md): Used in collation assignment for hypothetical set aggregates

## Notes and Other Information
- Part of PostgreSQL's system catalog lookup utilities in lsyscache.c
- Throws ERROR if function does not exist, ensuring strict validation
- Returns InvalidOid (0) for non-variadic functions
- Critical for handling functions that accept variable numbers of arguments
- Used primarily in aggregate function processing and parser collation assignment
- The variadic type determines how multiple trailing arguments are interpreted
- Essential for PostgreSQL's support of SQL functions with VARIADIC parameters

## Simplified Source

```c
Oid get_func_variadictype(Oid funcid) {
    HeapTuple tp;
    Oid result;

    // Look up function in system cache
    tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tp)) {
        elog(ERROR, "cache lookup failed for function %u", funcid);
    }

    // Extract variadic type from pg_proc entry
    result = ((Form_pg_proc) GETSTRUCT(tp))->provariadic;

    // Release cache entry
    ReleaseSysCache(tp);

    return result;  // Returns InvalidOid for non-variadic functions
}
```