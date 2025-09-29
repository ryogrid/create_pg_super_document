# get_typ_typrelid

## Location
[src/backend/utils/cache/lsyscache.c:2731-2758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2731-L2758)

## Overview
A system cache utility function that retrieves the relation OID (typrelid) associated with a given PostgreSQL type OID, returning InvalidOid for non-complex types.

## Definition
```c
Oid get_typ_typrelid(Oid typid)
```

## Detailed Description
This function performs a system catalog lookup to fetch the typrelid field from the pg_type system catalog. The typrelid field contains the OID of the relation (table) that defines a complex type's structure. For simple types (like integers, text, etc.), this field is InvalidOid (0). For complex types (composite types, table row types), it points to the pg_class entry that describes the type's structure. Unlike some other lsyscache functions, this function returns InvalidOid rather than throwing an error when the type is not found.

## Parameters / Member Variables
- `typid`: The OID (object identifier) of the PostgreSQL type to look up

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_type
  - InvalidOid
- Called from (representative examples):
  - [find_expr_references_walker](../f/find_expr_references_walker.md)
  - [process_function_rte_ref](../p/process_function_rte_ref.md)
  - [CheckAttributeType](../C/CheckAttributeType.md)
  - [ATPostAlterTypeCleanup](../A/ATPostAlterTypeCleanup.md)
  - [processIndirection](../p/processIndirection.md)

## Notes and Other Information
This function is essential for distinguishing between simple and complex types in PostgreSQL. It's commonly used in dependency tracking, type validation, and situations where the system needs to understand the internal structure of a type. The function's graceful handling of invalid type OIDs (returning InvalidOid instead of throwing an error) makes it suitable for defensive programming contexts where type existence is uncertain.

## Simplified Source

```c
Oid get_typ_typrelid(Oid typid) {
    // Look up the type in the system cache
    HeapTuple tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

    if (HeapTupleIsValid(tp)) {
        // Extract the typrelid field from the pg_type tuple
        Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
        Oid result = typtup->typrelid;

        // Clean up and return the relation OID
        ReleaseSysCache(tp);
        return result;
    }

    // Type not found - return invalid OID
    return InvalidOid;
}
```