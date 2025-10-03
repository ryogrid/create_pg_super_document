# RemovePublicationRelById

## Location
[src/backend/commands/publicationcmds.c:1441-1481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1441-L1481)

## Overview
RemovePublicationRelById removes a relation from a publication using its publication-relation mapping OID, handling partition hierarchy invalidation and catalog cleanup.

## Definition

```c
void
RemovePublicationRelById(Oid proid)
```
## Detailed Description
This function removes a specific publication-relation mapping from the pg_publication_rel catalog table using the mapping's OID. It performs comprehensive cache invalidation that extends beyond the explicitly referenced relation to include all partitions in the partition hierarchy, ensuring that logical replication remains consistent when partitioned tables are involved. The function is typically called during dependency-driven cascading deletions when publications or relations are dropped.

## Parameters / Member Variables
- : OID of the publication-relation mapping entry in pg_publication_rel to be removed

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (catalog access)
  - [SearchSysCache1](../S/SearchSysCache1.md) (cache lookup)
  - [GetPubPartitionOptionRelations](../G/GetPubPartitionOptionRelations.md) (partition hierarchy resolution)
  - [InvalidatePublicationRels](../I/InvalidatePublicationRels.md) (cache invalidation)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (catalog modification)
  - [ReleaseSysCache](ReleaseSysCache.md) (cache cleanup)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (dependency system)

## Notes and Other Information
- Uses RowExclusiveLock on PublicationRelRelationId for safe concurrent access
- Implements comprehensive partition handling by invalidating entire partition hierarchies
- Part of PostgreSQL's dependency system for cascading deletions
- Critical for maintaining cache consistency in logical replication
- Uses PUBLICATION_PART_ALL to include all partition levels in invalidation
- Error handling includes cache lookup failure detection with elog(ERROR)
- Essential component of publication cleanup during DROP operations

## Simplified Source

```c
void RemovePublicationRelById(Oid proid)
{
    Relation rel;
    HeapTuple tup;
    Form_pg_publication_rel pubrel;
    List *relids = NIL;

    // Open publication-relation catalog table
    rel = table_open(PublicationRelRelationId, RowExclusiveLock);

    // Look up the publication-relation mapping
    tup = SearchSysCache1(PUBLICATIONREL, ObjectIdGetDatum(proid));

    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for publication table %u", proid);

    pubrel = (Form_pg_publication_rel) GETSTRUCT(tup);

    // Get all relations in partition hierarchy for cache invalidation
    // This includes the relation itself and all its partitions
    relids = GetPubPartitionOptionRelations(relids, PUBLICATION_PART_ALL,
                                           pubrel->prrelid);

    // Invalidate relation cache for all affected relations
    InvalidatePublicationRels(relids);

    // Delete the publication-relation mapping
    CatalogTupleDelete(rel, &tup->t_self);

    ReleaseSysCache(tup);
    table_close(rel, RowExclusiveLock);
}
```