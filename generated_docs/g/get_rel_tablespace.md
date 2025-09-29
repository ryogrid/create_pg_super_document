# get_rel_tablespace

## Location
[src/backend/utils/cache/lsyscache.c:2054-2077](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2054-L2077)

## Overview
Returns the pg_tablespace OID associated with a given relation, indicating which tablespace stores the relation's data files.

## Definition

```c
Oid
get_rel_tablespace(Oid relid)
```
## Detailed Description
This function retrieves the tablespace OID for a specified relation from the system catalog. Tablespaces in PostgreSQL allow database administrators to define locations in the file system where database objects can be stored. The function performs a system cache lookup on the pg_class catalog using the relation OID and extracts the reltablespace field.

It's important to note that InvalidOid can have two meanings: either the relation doesn't exist, or the relation is stored in the database's default tablespace. When a relation is created without specifying a tablespace, it uses the default tablespace, and the reltablespace field is set to InvalidOid (0).

## Parameters / Member Variables
- : The OID of the relation whose tablespace OID is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_class (pg_class catalog structure)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)

- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)
  - [pg_get_indexdef_worker](../p/pg_get_indexdef_worker.md)
  - [pg_get_constraintdef_worker](../p/pg_get_constraintdef_worker.md)

## Notes and Other Information
- InvalidOid result can mean either nonexistent relation or default tablespace usage
- Essential for tablespace management and storage location determination
- Used in DDL operations like CREATE TABLE and CREATE INDEX
- Important for generating SQL definitions of database objects
- Helps PostgreSQL locate physical storage files for relations
- Default tablespace relations have reltablespace = InvalidOid
- Located in src/backend/utils/cache/lsyscache.c:2054-2077

## Simplified Source

```c
Oid
get_rel_tablespace(Oid relid)
{
    HeapTuple tp;

    // Look up relation in system cache
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
        Oid result;

        // Extract tablespace OID from pg_class tuple
        result = reltup->reltablespace;
        ReleaseSysCache(tp);
        return result;
    } else {
        return InvalidOid; // Relation not found
    }
}
```