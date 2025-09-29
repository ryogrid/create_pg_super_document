# get_typcollation

## Location
[src/backend/utils/cache/lsyscache.c:3056-3080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3056-L3080)

## Overview
Retrieves the default collation OID (typcollation) for a given PostgreSQL data type, which determines the sorting and comparison behavior for values of that type.

## Definition
```c
Oid get_typcollation(Oid typid)
```

## Detailed Description
This function looks up the default collation for a specified type from the PostgreSQL system catalog. Collations define the rules for sorting and comparing text data, including case sensitivity, accent sensitivity, and locale-specific ordering rules. The function searches the pg_type system table and returns the OID of the type's default collation, or InvalidOid if the type doesn't have a collation or if the type doesn't exist.

This function is widely used throughout PostgreSQL's parser, optimizer, and executor components when working with collatable types (primarily text types) to ensure proper collation-aware operations.

## Parameters / Member Variables
- `typid`: The OID of the PostgreSQL data type for which the collation information is needed

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (extract struct from heap tuple)
  - Form_pg_type (pg_type catalog structure)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release cache reference)
- Called from (representative examples):
  - [assign_collations_walker](../a/assign_collations_walker.md) (parser collation assignment)
  - [transformAssignmentSubscripts](../t/transformAssignmentSubscripts.md) (parse target processing)
  - [GetColumnDefCollation](../G/GetColumnDefCollation.md) (type parsing utilities)
  - [resolve_polymorphic_tupdesc](../r/resolve_polymorphic_tupdesc.md) (function API utilities)
  - [type_is_collatable](../t/type_is_collatable.md) (collation capability checking)

## Notes and Other Information
- Returns InvalidOid if the type OID is not found in the system catalog, providing graceful error handling
- Not all types have collations - only collatable types (mainly text-based types) will return valid OIDs
- Used extensively in the parser to assign appropriate collations to expressions and operations
- Essential for internationalization and locale-aware text processing in PostgreSQL
- Part of PostgreSQL's comprehensive collation support system introduced in version 9.1
- Declared in src/include/utils/lsyscache.h as part of the public API
- Works in conjunction with type_is_collatable() to determine if a type supports collation

## Simplified Source

```c
Oid get_typcollation(Oid typid)
{
    HeapTuple tp;

    // Look up the type in the system cache
    tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(tp))
    {
        // Extract the pg_type structure from the tuple
        Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
        Oid result;

        // Get the collation OID from the type structure
        result = typtup->typcollation;

        // Release the cache entry
        ReleaseSysCache(tp);
        return result;
    }
    else
    {
        // Type not found - return invalid OID
        return InvalidOid;
    }
}
```