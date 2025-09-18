# GetAllSchemaPublicationRelations

## Location
[src/backend/catalog/pg_publication.c:982-1005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L982-L1005)

## Overview
Retrieves a consolidated list of all relation OIDs published by a specific publication that uses "FOR TABLES IN SCHEMA" syntax, aggregating relations from all schemas included in the publication.

## Definition
List *GetAllSchemaPublicationRelations(Oid pubid, PublicationPartOpt pub_partopt)

## Detailed Description
This function serves as an aggregation layer that combines all publishable relations from every schema that belongs to a specific publication. It first retrieves the list of schemas associated with the publication, then iterates through each schema to collect all publishable relations within those schemas. The function handles the "FOR TABLES IN SCHEMA" publication feature, which allows publications to include entire schemas rather than individual tables.

The function delegates the actual relation discovery to GetSchemaPublicationRelations for each schema, ensuring consistent handling of publication partition options and relation filtering across all schemas in the publication.

## Parameters / Member Variables
- : The OID of the publication for which to retrieve all schema-based relations
- : Publication partition option that determines how partitioned tables are handled across all schemas

## Dependencies
- Functions called/Symbols referenced:
  - [GetPublicationSchemas](GetPublicationSchemas.md): Retrieves the list of schema OIDs associated with the publication
  - [GetSchemaPublicationRelations](GetSchemaPublicationRelations.md): Gets publishable relations for each individual schema
  - [list_concat](../l/list_concat.md): Concatenates relation lists from multiple schemas
  - lfirst_oid: Extracts OID from list cell during iteration
- Called from (representative examples):
  - [AlterPublicationOptions](../A/AlterPublicationOptions.md): Updates publication when options change
  - NUM_PUBLICATION_TABLES_ELEM: Counts total relations in schema-based publications

## Notes and Other Information
- Returns NIL (empty list) if the publication has no schemas or if no publishable relations exist in those schemas
- Efficiently aggregates relations from multiple schemas into a single list for the publication
- Part of PostgreSQL's logical replication support for schema-level publication definitions
- The function maintains the publication partition options consistently across all schemas
- Essential for determining the complete set of tables that will be replicated for schema-based publications