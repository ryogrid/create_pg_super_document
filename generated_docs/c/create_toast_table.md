# create_toast_table

## Location
[src/backend/catalog/toasting.c:127-400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/toasting.c#L127-L400)

## Overview
Creates a TOAST (The Oversized-Attribute Storage Technique) table and its associated index for a given relation to handle storage of large attribute values that exceed PostgreSQL's page size limits.

## Definition


## Detailed Description
This is the internal workhorse function for creating TOAST tables in PostgreSQL. It performs comprehensive setup of a TOAST table structure including:

1. **Validation**: Checks if the relation already has a TOAST table and whether one is actually needed
2. **Binary Upgrade Handling**: Special logic for pg_upgrade scenarios to maintain consistency with old cluster TOAST table presence
3. **Table Creation**: Creates the TOAST table with a 3-column structure (chunk_id, chunk_seq, chunk_data)
4. **Index Creation**: Creates a unique btree index on (chunk_id, chunk_seq) for efficient chunk retrieval
5. **Catalog Updates**: Updates the parent table's pg_class entry to reference the new TOAST table
6. **Dependency Registration**: Establishes dependency relationships so TOAST table is dropped when parent is dropped

The function handles both normal operation mode and bootstrap mode with different update strategies for catalog modifications. It also manages proper namespace assignment (pg_toast for regular tables, temp-toast namespace for temporary tables) and ensures TOAST tables inherit sharing and mapping properties from their parent relations.

## Parameters / Member Variables
- : The relation (table) for which to create a TOAST table, must be already opened and locked
- : OID to assign to the TOAST table (normally InvalidOid except during bootstrap)
- : OID to assign to the TOAST table's index (normally InvalidOid except during bootstrap)
- : Relation options (storage parameters) to apply to the TOAST table
- : Lock mode held on the parent relation (should be AccessExclusiveLock for safety)
- : Whether to verify that the lockmode is sufficient (performs safety check)
- : OID of the old TOAST table during binary upgrade operations

## Dependencies
- Functions called/Symbols referenced:
  - [needs_toast_table](../n/needs_toast_table.md)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)  
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [table_relation_toast_am](../t/table_relation_toast_am.md)
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - index_create
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md)
  - [systable_inplace_update_finish](../s/systable_inplace_update_finish.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - CommandCounterIncrement
- Called from (representative examples):
  - [CheckAndCreateToastTable](../C/CheckAndCreateToastTable.md)
  - [BootstrapToastTable](../B/BootstrapToastTable.md)

## Notes and Other Information
- Returns true if a TOAST table was created, false if one already existed or wasn't needed
- The TOAST table uses a fixed 3-column schema: chunk_id (OID), chunk_seq (int4), chunk_data (bytea)
- All TOAST table columns use PLAIN storage to prevent recursive toasting
- The unique index on (chunk_id, chunk_seq) serves both uniqueness enforcement and query optimization
- During bootstrap mode, uses in-place catalog updates instead of transactional updates
- Handles special cases for shared relations, temporary relations, and binary upgrade scenarios
- The function includes extensive comments explaining the rationale for the two-column index design