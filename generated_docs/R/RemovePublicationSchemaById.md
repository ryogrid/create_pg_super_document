# RemovePublicationSchemaById

## Location
[src/backend/commands/publicationcmds.c:1511-1548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1511-L1548)

## Overview
RemovePublicationSchemaById removes a schema from a publication using its publication-schema mapping OID, performing comprehensive cache invalidation for all affected relations and partitions.

## Definition

```c
void
RemovePublicationSchemaById(Oid psoid)
```
## Detailed Description
This function removes a specific publication-schema mapping from the pg_publication_namespace catalog table using the mapping's OID. It performs extensive cache invalidation by identifying all publishable relations within the schema and invalidating their cache entries, including all partitions in partition hierarchies. This comprehensive invalidation approach ensures that logical replication remains consistent when schema-based publications are modified. The function is primarily invoked by the dependency system during cascading deletions.

## Parameters / Member Variables
- `psoid`: OID of the publication-schema mapping entry in pg_publication_namespace to be removed
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md) (catalog access)
  - [SearchSysCache1](../S/SearchSysCache1.md) (cache lookup)
  - [GetSchemaPublicationRelations](../G/GetSchemaPublicationRelations.md) (relation enumeration)
  - [InvalidatePublicationRels](../I/InvalidatePublicationRels.md) (cache invalidation)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (catalog modification)
  - [ReleaseSysCache](ReleaseSysCache.md) (cache cleanup)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (dependency system)

## Notes and Other Information
- Uses RowExclusiveLock on PublicationNamespaceRelationId for concurrent access safety
- Implements comprehensive partition handling via PUBLICATION_PART_ALL flag
- Leverages GetSchemaPublicationRelations to enumerate all publishable relations in the schema
- Part of PostgreSQL's dependency system for cascading schema publication removal
- Critical for maintaining cache consistency across schema-level publication changes
- Error handling includes cache lookup failure detection with detailed error messages
- More complex than individual relation removal due to schema-wide impact assessment

## Simplified Source

```c
void
RemovePublicationSchemaById(Oid psoid)
{
    Relation rel;
    HeapTuple tup;
    List *schemaRels = NIL;
    Form_pg_publication_namespace pubsch;

    // Open publication namespace catalog with exclusive lock
    rel = table_open(PublicationNamespaceRelationId, RowExclusiveLock);

    // Find the publication schema mapping tuple
    tup = SearchSysCache1(PUBLICATIONNAMESPACE, ObjectIdGetDatum(psoid));

    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for publication schema %u", psoid);

    pubsch = (Form_pg_publication_namespace) GETSTRUCT(tup);

    // Get all relations in this schema for cache invalidation
    // Includes all partitions to ensure comprehensive invalidation
    schemaRels = GetSchemaPublicationRelations(pubsch->pnnspid, PUBLICATION_PART_ALL);
    InvalidatePublicationRels(schemaRels);

    // Remove the mapping tuple from catalog
    CatalogTupleDelete(rel, &tup->t_self);

    // Cleanup
    ReleaseSysCache(tup);
    table_close(rel, RowExclusiveLock);
}
```