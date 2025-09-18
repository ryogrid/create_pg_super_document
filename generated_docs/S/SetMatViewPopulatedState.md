# SetMatViewPopulatedState

## Location
src/backend/commands/matview.c: 79 - 120

## Overview
SetMatViewPopulatedState marks a materialized view as populated or unpopulated by updating its relispopulated flag in the pg_class system catalog.

## Definition


## Detailed Description
This function updates the relispopulated field in the pg_class system catalog for a materialized view relation. The function performs a catalog update that triggers shared invalidation messages to other backends, ensuring that all processes rebuild their relation cache entries to reflect the new populated state. This is critical for maintaining consistency across the database cluster when materialized views are refreshed or initially populated.

The function requires that the caller holds an appropriate lock on the relation and verifies that the relation is indeed a materialized view through an assertion. After updating the catalog, it advances the command counter to make the change locally visible within the same transaction.

## Parameters / Member Variables
- : The Relation structure representing the materialized view to update
- : Boolean flag indicating whether the materialized view should be marked as populated (true) or unpopulated (false)

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_MATVIEW (constant for materialized view relation kind)
  - table_open (opens system catalog relation)
  - SearchSysCacheCopy1 (retrieves tuple from system cache)
  - RelationGetRelid (gets OID from relation)
  - GETSTRUCT (macro to access tuple data)
  - Form_pg_class (structure type for pg_class tuples)
  - CatalogTupleUpdate (updates system catalog tuple)
  - heap_freetuple (frees heap tuple memory)
  - table_close (closes system catalog relation)
  - CommandCounterIncrement (advances command counter)

- Called from (representative examples):
  - intorel_startup (when creating materialized views via CREATE TABLE AS)
  - RefreshMatViewByOid (when refreshing materialized view data)

## Notes and Other Information
- The caller must hold an appropriate lock on the materialized view relation before calling this function
- The function includes an assertion to verify the relation is of type RELKIND_MATVIEW
- The catalog update triggers shared invalidation messages to ensure all backends update their relation caches
- Command counter increment ensures the updated state is visible within the same transaction
- This function is essential for the materialized view refresh mechanism in PostgreSQL