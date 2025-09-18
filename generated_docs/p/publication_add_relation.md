# publication_add_relation

## Location
src/backend/catalog/pg_publication.c: 358 - 480

## Overview
Inserts a new publication/relation mapping into the pg_publication_rel catalog table, establishing a relationship between a publication and a table with optional column list and WHERE clause qualifications.

## Definition


## Detailed Description
This function creates a new entry in the pg_publication_rel system catalog to associate a relation (table) with a publication. It handles the complete process of validating the relation, translating column specifications, creating catalog entries, and establishing proper dependencies. The function supports conditional insertion (IF NOT EXISTS semantics) and can include row filtering through WHERE clauses and column filtering through explicit column lists.

The function performs several key operations:
1. Validates that the relation can be added to the publication
2. Checks for existing duplicates and handles them based on the if_not_exists parameter
3. Translates column names to attribute numbers and validates column specifications
4. Creates the catalog tuple with appropriate values for relation ID, publication ID, WHERE clause, and column list
5. Establishes dependency relationships between the publication entry and referenced objects
6. Invalidates relation caches to ensure publication information is properly updated

## Parameters / Member Variables
- : Object ID of the publication to which the relation should be added
- : PublicationRelInfo structure containing the relation, optional column list, and optional WHERE clause
- : Boolean flag indicating whether to silently skip if the relation is already a member of the publication

## Dependencies
- Functions called/Symbols referenced:
  - [GetPublication](../G/GetPublication.md)
  - SearchSysCacheExists2
  - [check_publication_add_relation](../c/check_publication_add_relation.md)
  - [publication_translate_columns](publication_translate_columns.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [nodeToString](../n/nodeToString.md)
  - [buildint2vector](../b/buildint2vector.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - ObjectAddressSet
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md)
  - ObjectAddressSubSet
  - [GetPubPartitionOptionRelations](../G/GetPubPartitionOptionRelations.md)
  - [InvalidatePublicationRels](../I/InvalidatePublicationRels.md)
- Called from (representative examples):
  - [PublicationAddTables](../P/PublicationAddTables.md) (src/backend/commands/publicationcmds.c:1765)

## Notes and Other Information
- The function uses RowExclusiveLock on the pg_publication_rel catalog to ensure consistency
- Duplicate detection is performed for better error messages, but the real protection comes from the unique key constraint on the catalog
- For partitioned tables, the function invalidates all partitions in the hierarchy since child tables are implicitly published when parent tables are published
- Column list validation ensures only allowed columns (no system or generated columns) are included
- Dependencies are established not only on the publication and relation, but also on individual columns and objects referenced in WHERE clauses
- Returns an ObjectAddress pointing to the newly created pg_publication_rel entry
- Location: src/backend/catalog/pg_publication.c:358-480