# CreatePublication

## Location
[src/backend/commands/publicationcmds.c:728-870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L728-L870)

## Overview
CreatePublication creates a new logical replication publication in PostgreSQL, which defines a set of tables or schemas whose changes can be replicated to subscribers.

## Definition


## Detailed Description
CreatePublication is the core function responsible for creating a new publication object in PostgreSQL's logical replication system. It performs comprehensive validation, creates the catalog entry, and associates the specified tables or schemas with the publication. The function handles both explicit table lists and schema-based publications, with special handling for "FOR ALL TABLES" publications that require superuser privileges.

The function validates permissions (requiring CREATE privilege on the database and superuser for certain publication types), ensures unique publication names, parses publication options, creates the catalog tuple, and establishes object dependencies. It also handles the association of tables and schemas with the publication, including WHERE clause transformation and column list validation.

## Parameters / Member Variables
- : ParseState containing parsing context and source text information
- : CreatePublicationStmt structure containing the publication creation command details including name, options, and object specifications

## Dependencies
- Functions called/Symbols referenced:
  - [object_aclcheck](../o/object_aclcheck.md): Permission checking for database CREATE privilege
  - [parse_publication_options](../p/parse_publication_options.md): Parses publication-specific options like publish actions
  - [ObjectsInPublicationToOids](../O/ObjectsInPublicationToOids.md): Converts publication objects to relation and schema OID lists
  - [TransformPubWhereClauses](../T/TransformPubWhereClauses.md): Processes WHERE clauses for publication relations
  - [CheckPubRelationColumnList](CheckPubRelationColumnList.md): Validates column specifications for publication relations
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md): Records ownership dependency for the publication
  - [heap_form_tuple](../h/heap_form_tuple.md)/heap_freetuple: Tuple creation and cleanup
  - [CatalogTupleInsert](CatalogTupleInsert.md): Inserts the publication tuple into pg_publication catalog
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processing function

## Notes and Other Information
- Requires CREATE privilege on the database for basic publications
- FOR ALL TABLES and FOR TABLES IN SCHEMA publications require superuser privileges
- Issues a WARNING if wal_level is not set to logical, as this is required for logical replication
- Invalidates relation cache for FOR ALL TABLES publications to rebuild publication information
- Supports both explicit table lists and schema-based table inclusion
- Handles partition root publishing preferences through publish_via_partition_root option