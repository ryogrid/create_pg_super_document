# get_typsubscript

## Location
[src/backend/utils/cache/lsyscache.c:3097-3129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3097-L3129)

## Overview
Retrieves the subscripting handler function OID for a given PostgreSQL data type, which enables array-like subscripting operations on that type.

## Definition
```c
RegProcedure get_typsubscript(Oid typid, Oid *typelemp)
```

## Detailed Description
This function looks up a type's subscripting handler in the system catalog (pg_type table). The subscripting handler is a function that implements subscripting operations (like array[index]) for the type. The function performs a system cache lookup to find the type's entry and extracts the `typsubscript` field. Additionally, it can optionally return the type's element type OID (`typelem`) to save callers from making an additional catalog lookup.

## Parameters / Member Variables
- `typid`: The OID of the PostgreSQL data type to look up
- `typelemp`: Optional output parameter; if not NULL, receives the type's element type OID (`typelem` field)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from (representative examples):
  - [getSubscriptingRoutines](getSubscriptingRoutines.md)

## Notes and Other Information
- Returns `InvalidOid` if the type doesn't exist or has no subscripting handler
- The subscripting handler enables custom subscripting behavior for user-defined types
- Built-in array types have subscripting handlers that implement standard array indexing
- The `typelemp` parameter optimization reduces system catalog lookups for callers who need both pieces of information
- Part of PostgreSQL's extensible type system that allows custom subscripting operations
- Located in `src/backend/utils/cache/lsyscache.c:3097-3129`

## Simplified Source

```c
RegProcedure get_typsubscript(Oid typid, Oid *typelemp) {
    HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(tp)) {
        Form_pg_type typform = (Form_pg_type) GETSTRUCT(tp);
        RegProcedure handler = typform->typsubscript;

        // Optionally return element type OID
        if (typelemp)
            *typelemp = typform->typelem;

        ReleaseSysCache(tp);
        return handler;
    } else {
        // Type not found
        if (typelemp)
            *typelemp = InvalidOid;
        return InvalidOid;
    }
}
```