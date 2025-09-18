# RemovePublicationSchemaById

## Location
src/backend/commands/publicationcmds.c: 1511 - 1548

## Overview
RemovePublicationSchemaById removes a schema from a publication using its publication-schema mapping OID, performing comprehensive cache invalidation for all affected relations and partitions.

## Definition


## Detailed Description
This function removes a specific publication-schema mapping from the pg_publication_namespace catalog table using the mapping's OID. It performs extensive cache invalidation by identifying all publishable relations within the schema and invalidating their cache entries, including all partitions in partition hierarchies. This comprehensive invalidation approach ensures that logical replication remains consistent when schema-based publications are modified. The function is primarily invoked by the dependency system during cascading deletions.

## Parameters / Member Variables
- : OID of the publication-schema mapping entry in pg_publication_namespace to be removed

## Dependencies
- Functions called/Symbols referenced:
  - table_open (catalog access)
  - SearchSysCache1 (cache lookup)
  - GetSchemaPublicationRelations (relation enumeration)
  - InvalidatePublicationRels (cache invalidation)
  - CatalogTupleDelete (catalog modification)
  - ReleaseSysCache (cache cleanup)
- Called from (representative examples):
  - doDeletion (dependency system)

## Notes and Other Information
- Uses RowExclusiveLock on PublicationNamespaceRelationId for concurrent access safety
- Implements comprehensive partition handling via PUBLICATION_PART_ALL flag
- Leverages GetSchemaPublicationRelations to enumerate all publishable relations in the schema
- Part of PostgreSQL's dependency system for cascading schema publication removal
- Critical for maintaining cache consistency across schema-level publication changes
- Error handling includes cache lookup failure detection with detailed error messages
- More complex than individual relation removal due to schema-wide impact assessment