# get_rel_relkind

## Location
[src/backend/utils/cache/lsyscache.c:2003-2026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2003-L2026)

## Overview
Returns the relkind (relation kind) character associated with a given relation, indicating the type of database object (table, index, view, etc.).

## Definition

```c
char
get_rel_relkind(Oid relid)
```
## Detailed Description
This function retrieves the relation kind (relkind) for a specified relation from the system catalog. The relkind field is a single character that identifies the type of relation object in PostgreSQL's catalogs. Common relkind values include 'r' for regular tables, 'i' for indexes, 'v' for views, 'S' for sequences, 'f' for foreign tables, 'p' for partitioned tables, and others.

The function performs a system cache lookup on the pg_class catalog using the relation OID and extracts the relkind field. This information is critical for determining how to handle different types of database objects in various PostgreSQL operations.

## Parameters / Member Variables
- `relid`: The OID of the relation whose kind is to be retrieved
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_class (pg_class catalog structure)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)

- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md)
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md)
  - [get_object_type](get_object_type.md)
  - [ReindexIndex](../R/ReindexIndex.md)
  - [LockTableCommand](../L/LockTableCommand.md)
  - [RenameRelation](../R/RenameRelation.md)
  - [CreateTriggerFiringOn](../C/CreateTriggerFiringOn.md)
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md)
  - [has_sequence_privilege_name_name](../h/has_sequence_privilege_name_name.md)
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md)

## Notes and Other Information
- Returns '\0' (null character) if the relation does not exist
- Critical for type checking and validation in many PostgreSQL operations
- Common relkind values: 'r' (table), 'i' (index), 'v' (view), 'S' (sequence), 'f' (foreign table), 'p' (partitioned table)
- Used extensively throughout PostgreSQL for relation type discrimination
- Essential for permission checking, DDL operations, and catalog management
- Located in src/backend/utils/cache/lsyscache.c:2003-2026

## Simplified Source

```c
char get_rel_relkind(Oid relid) {
    HeapTuple tp;

    // Look up relation in system cache
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

    if (HeapTupleIsValid(tp)) {
        // Extract relation structure and get relkind
        Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
        char result = reltup->relkind;

        ReleaseSysCache(tp);
        return result;
    } else {
        return '\0';  // Relation not found
    }
}
```