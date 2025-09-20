# get_rel_namespace

## Location
[src/backend/utils/cache/lsyscache.c:1952-1978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1952-L1978)

## Overview
Returns the pg_namespace OID associated with a given relation, providing namespace information for database relations.

## Definition

```c
Oid
get_rel_namespace(Oid relid)
```
## Detailed Description
This function retrieves the namespace (schema) OID for a specified relation from the system catalog. It performs a system cache lookup on the pg_class catalog using the relation OID, extracts the relnamespace field from the relation tuple, and returns the namespace OID. If the relation is not found, it returns InvalidOid.

The function uses PostgreSQL's system cache mechanism for efficient catalog lookups, which helps avoid repeated disk I/O for frequently accessed catalog information.

## Parameters / Member Variables
- : The OID of the relation whose namespace is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_class (pg_class catalog structure)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)

- Called from (representative examples):
  - [reindex_relation](../r/reindex_relation.md)
  - [GetTopMostAncestorInPublication](../G/GetTopMostAncestorInPublication.md)
  - [swap_relation_files](../s/swap_relation_files.md)
  - [ExplainTargetRel](../E/ExplainTargetRel.md)
  - [ExecCheckXactReadOnly](../E/ExecCheckXactReadOnly.md)
  - [do_autovacuum](../d/do_autovacuum.md)
  - [RelationBuildPublicationDesc](../R/RelationBuildPublicationDesc.md)

## Notes and Other Information
- This function is part of PostgreSQL's low-level system cache utility functions
- Returns InvalidOid if the relation does not exist
- Uses system cache for performance optimization
- Critical for namespace/schema resolution in various PostgreSQL operations
- Located in src/backend/utils/cache/lsyscache.c:1952-1978