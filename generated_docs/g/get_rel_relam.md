# get_rel_relam

## Location
[src/backend/utils/cache/lsyscache.c:2100-2119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2100-L2119)

## Overview
Returns the access method OID (relam) associated with a given relation, identifying which access method is used to store and access the relation's data.

## Definition
```c
Oid get_rel_relam(Oid relid)
```

## Detailed Description
This function retrieves the access method identifier (relam) of a relation from the PostgreSQL system catalog (pg_class). The access method determines how data is physically stored and accessed for the relation. Common access methods include heap (standard table storage), btree (B-tree indexes), hash (hash indexes), and others. For regular tables, this is typically the heap access method, while for indexes it indicates the specific index type.

The function performs a system cache lookup to efficiently retrieve this information without directly accessing the catalog table.

## Parameters / Member Variables
- `relid`: The OID (Object Identifier) of the relation whose access method is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - elog (error logging)
  - GETSTRUCT (macro to extract structure from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_class (pg_class tuple structure)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)

## Notes and Other Information
- The function will throw an ERROR if the relation OID is not found in the system catalog
- Uses PostgreSQL's system cache for efficient lookup
- The relam field is crucial for determining how to physically access and manipulate the relation's data
- For tables, relam typically points to the heap access method
- For indexes, relam indicates the specific index access method (btree, hash, gin, gist, etc.)
- Part of the low-level system cache API (lsyscache.c) that provides convenient access to catalog information

## Simplified Source

```c
Oid
get_rel_relam(Oid relid)
{
    HeapTuple tp;
    Form_pg_class reltup;
    Oid result;

    // Look up relation in system cache
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for relation %u", relid);

    // Extract access method OID from pg_class tuple
    reltup = (Form_pg_class) GETSTRUCT(tp);
    result = reltup->relam;
    ReleaseSysCache(tp);

    return result;
}
```