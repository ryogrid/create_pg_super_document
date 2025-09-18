# GetSchemaPublicationRelations

## Location
[src/backend/catalog/pg_publication.c:925-981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L925-L981)

## Overview
Retrieves a list of publishable relation OIDs within a specified schema, handling both regular tables and partitioned tables according to publication partition options.

## Definition
List *GetSchemaPublicationRelations(Oid schemaid, PublicationPartOpt pub_partopt)

## Detailed Description
This function scans the pg_class system catalog to find all publishable relations (tables) within the specified schema. It handles different types of relations appropriately for logical replication, including regular tables and partitioned tables. For partitioned tables, it considers the publication partition options to determine whether to include child partitions that may reside in different schemas.

The function performs a catalog scan on the pg_class relation, filtering by schema namespace, and then applies additional checks to determine if each relation is publishable. For regular tables (RELKIND_RELATION), it directly adds them to the result. For partitioned tables (RELKIND_PARTITIONED_TABLE), it recursively includes partitions based on the provided publication partition options.

## Parameters / Member Variables
- : The OID of the schema to scan for publishable relations
- : Publication partition option that determines how partitioned tables are handled (e.g., whether to include child partitions)

## Dependencies
- Functions called/Symbols referenced:
  - table_open: Opens the pg_class system catalog relation
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md): Begins a catalog scan with specified scan keys
  - [heap_getnext](../h/heap_getnext.md): Retrieves the next tuple from the scan
  - [is_publishable_class](../i/is_publishable_class.md): Checks if a relation class is publishable for logical replication
  - [get_rel_relkind](../g/get_rel_relkind.md): Gets the relation kind (table, partitioned table, etc.)
  - [GetPubPartitionOptionRelations](GetPubPartitionOptionRelations.md): Recursively gets partition relations based on publication options
  - [list_concat_unique_oid](../l/list_concat_unique_oid.md): Concatenates lists while avoiding duplicates
  - [table_endscan](../t/table_endscan.md): Ends the table scan
  - table_close: Closes the relation
- Called from (representative examples):
  - [publication_add_schema](../p/publication_add_schema.md): Adds relations when schema is added to publication
  - [GetAllSchemaPublicationRelations](GetAllSchemaPublicationRelations.md): Gets relations for all schemas in publications
  - [RemovePublicationSchemaById](../R/RemovePublicationSchemaById.md): Handles relation cleanup when removing schema from publication

## Notes and Other Information
- Requires a valid schema OID as input (checked with Assert)
- Handles the complexity of partitioned tables where child partitions may be in different schemas
- Uses AccessShareLock for safe concurrent access to system catalogs
- Returns NIL (empty list) if no publishable relations are found in the schema
- Part of PostgreSQL's logical replication infrastructure for schema-level publication management