# IsCatalogRelation

## Location
[src/backend/catalog/catalog.c:103-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/catalog.c#L103-L119)

## Overview
IsCatalogRelation determines whether a given relation is a true system catalog that was created during the bootstrap phase of initdb.

## Definition
```c
bool IsCatalogRelation(Relation relation)
```

## Detailed Description
This function identifies relations that are genuine system catalogs in PostgreSQL. Unlike IsSystemRelation, which includes toast tables of user relations, IsCatalogRelation only returns true for relations that were created during the database bootstrap phase. This includes system catalogs themselves, their indexes, and any associated TOAST tables and indexes.

The function serves as a wrapper around IsCatalogRelationOid, extracting the relation OID from the Relation structure. It's designed to be lightweight and does not perform any catalog accesses, making it safe to use in contexts where catalog lookups would be problematic or impossible.

## Parameters / Member Variables
- `relation`: A Relation structure representing the table/relation to be checked

## Dependencies
- Functions called/Symbols referenced:
  - [IsCatalogRelationOid](IsCatalogRelationOid.md)
  - RelationGetRelid (macro to extract OID from relation)
- Called from (representative examples):
  - [heap_multi_insert](../h/heap_multi_insert.md)
  - [index_create](../i/index_create.md)
  - [check_publication_add_relation](../c/check_publication_add_relation.md)
  - [needs_toast_table](../n/needs_toast_table.md)
  - [CacheInvalidateHeapTuple](../C/CacheInvalidateHeapTuple.md)

## Notes and Other Information
- Only identifies relations created during the bootstrap phase of initdb
- Includes catalogs themselves, their indexes, and TOAST tables/indexes of catalogs
- Does not perform catalog accesses, ensuring it's safe for use in various contexts
- More restrictive than IsSystemRelation - excludes toast tables of user relations
- Used in contexts where the distinction between true system catalogs and user relation toast tables matters
- The function is located in src/backend/catalog/catalog.c:103-119