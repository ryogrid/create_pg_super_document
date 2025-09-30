# get_rel_type_id

## Location
[src/backend/utils/cache/lsyscache.c:1979-2002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1979-L2002)

## Overview
Returns the pg_type OID associated with a given relation, providing type information for database relations that have corresponding composite types.

## Definition

```c
Oid
get_rel_type_id(Oid relid)
```
## Detailed Description
This function retrieves the type OID associated with a specified relation from the system catalog. It performs a system cache lookup on the pg_class catalog using the relation OID and extracts the reltype field from the relation tuple. The function is important because in PostgreSQL, tables and views can have associated composite types that represent their row structure.

It's crucial to note that not all pg_class entries have associated pg_type OIDs, so callers must check for InvalidOid results. This typically applies to indexes, sequences, and other relation types that don't have meaningful row types.

## Parameters / Member Variables
- : The OID of the relation whose associated type OID is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_class (pg_class catalog structure)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)

- Called from (representative examples):
  - [ExecAlterExtensionContentsRecurse](../E/ExecAlterExtensionContentsRecurse.md)
  - [makeWholeRowVar](../m/makeWholeRowVar.md)
  - [serialize_expr_stats](../s/serialize_expr_stats.md)

## Notes and Other Information
- Not all pg_class entries have associated pg_type OIDs - callers must handle InvalidOid
- Returns InvalidOid if the relation does not exist or has no associated type
- Essential for operations involving composite types and whole-row references
- Tables and views typically have associated types, while indexes and sequences do not
- Uses system cache for performance optimization
- Located in src/backend/utils/cache/lsyscache.c:1979-2002

## Simplified Source

```c
Oid get_rel_type_id(Oid relid) {
    // Look up relation in system cache
    HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

    if (HeapTupleIsValid(tp)) {
        // Extract the relation tuple and get its type OID
        Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
        Oid result = reltup->reltype;
        ReleaseSysCache(tp);
        return result;
    } else {
        // Relation not found
        return InvalidOid;
    }
}
```