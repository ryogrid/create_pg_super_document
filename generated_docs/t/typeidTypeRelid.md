# typeidTypeRelid

## Location
[src/backend/parser/parse_type.c:668-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L668-L688)

## Overview
Gets the associated relation OID (typrelid) for a given type OID, specifically for composite types.

## Definition

```c
Oid
typeidTypeRelid(Oid type_id)
```
## Detailed Description
This function looks up a type by its OID in the system catalog and returns the associated relation OID (typrelid field from pg_type). The typrelid field is only meaningful for composite types, which are types that have an underlying table or view structure. For non-composite types (like built-in types int4, text, etc.), this function returns InvalidOid.

The function performs a system cache lookup on the TYPEOID cache to efficiently retrieve the type information from pg_type catalog.

## Parameters / Member Variables
- `type_id`: The OID of the type to look up in the pg_type system catalog
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [typeInheritsFrom](typeInheritsFrom.md) (src/backend/catalog/pg_inherits.c:420)
  - [transformAssignmentIndirection](transformAssignmentIndirection.md) (src/backend/parser/parse_target.c:775)

## Notes and Other Information
- Returns InvalidOid for non-composite types
- Throws an ERROR if the type OID is not found in the system catalog
- Uses system cache for efficient lookup
- The typrelid field in pg_type points to the pg_class entry for composite types

## Simplified Source

```c
Oid
typeidTypeRelid(Oid type_id)
{
    HeapTuple typeTuple;
    Form_pg_type type;
    Oid result;

    // Look up type in system catalog
    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
    if (!HeapTupleIsValid(typeTuple))
        elog(ERROR, "cache lookup failed for type %u", type_id);

    // Extract relation OID from type structure
    type = (Form_pg_type) GETSTRUCT(typeTuple);
    result = type->typrelid;

    ReleaseSysCache(typeTuple);
    return result;
}
```