# check_publication_add_relation

## Location
src/backend/catalog/pg_publication.c: 59 - 97

## Overview
A static validation function that checks if a relation (table) can be added to a publication, throwing appropriate errors if the relation is not suitable for publication.

## Definition


## Detailed Description
This function performs validation checks to ensure that a given relation can be safely added to a logical replication publication. It enforces several restrictions:

1. **Relation Kind Validation**: Only regular tables (RELKIND_RELATION) and partitioned tables (RELKIND_PARTITIONED_TABLE) are allowed in publications.

2. **System Table Restriction**: System/catalog tables cannot be added to publications as they are not suitable for logical replication.

3. **Persistence Validation**: Tables with certain persistence characteristics are forbidden:
   - Temporary tables (RELPERSISTENCE_TEMP) cannot be published
   - Unlogged tables (RELPERSISTENCE_UNLOGGED) cannot be published

The function uses PostgreSQL's error reporting mechanism to provide detailed error messages when validation fails, including specific reasons why the relation cannot be published.

## Parameters / Member Variables
- : A Relation pointer to the table being validated for publication inclusion

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetForm
  - RelationGetRelationName  
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [IsCatalogRelation](../I/IsCatalogRelation.md)
  - ereport (error reporting)
- Constants referenced:
  - RELKIND_RELATION
  - RELKIND_PARTITIONED_TABLE
  - RELPERSISTENCE_TEMP
  - RELPERSISTENCE_UNLOGGED
- Called from:
  - [publication_add_relation](../p/publication_add_relation.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_publication.c compilation unit
- The function only performs validation and does not modify any state
- Error messages are standardized and include both the relation name and specific reason for rejection
- The function is designed to fail fast - it stops at the first validation error encountered
- Location: src/backend/catalog/pg_publication.c:59-97