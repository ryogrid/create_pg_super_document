# index_update_stats

## Location
[src/backend/catalog/index.c:2781-2939](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L2781-L2939)

## Overview
index_update_stats updates the pg_class catalog entry for a relation after CREATE INDEX or REINDEX operations, maintaining statistical metadata and triggering relcache invalidation.

## Definition

```c
static void
index_update_stats(Relation rel,
				   bool hasindex,
				   double reltuples)
```
## Detailed Description
index_update_stats is a critical internal function that updates statistical information in the pg_class catalog table following index creation or reindexing operations. The function uses non-transactional, in-place updates to safely modify relation metadata even during bootstrap mode or when reindexing system catalogs. It updates multiple statistics including relhasindex, reltuples, relpages, and relallvisible. The function includes special handling for empty tables to avoid premature vacuum appearance during CREATE TABLE operations. A key aspect of this function is ensuring that shared invalidation messages are sent to all backends, which triggers relcache updates across the system, notifying other processes about new indexes or updated statistics.

## Parameters / Member Variables
- : The relation (either index or its parent table) being updated
- : Boolean value to set for the relhasindex field in pg_class
- : New tuple count; if >= 0, updates reltuples and related statistics; if < 0, no change to tuple statistics

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_class (structure type)
  - RelationGetNumberOfBlocks (function)
  - RELKIND_INDEX (constant)
  - [visibilitymap_count](../v/visibilitymap_count.md) (function)
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md) (function)
  - [systable_inplace_update_finish](../s/systable_inplace_update_finish.md) (function)
  - [systable_inplace_update_cancel](../s/systable_inplace_update_cancel.md) (function)
  - [CacheInvalidateRelcacheByTuple](../C/CacheInvalidateRelcacheByTuple.md) (function)
  - [heap_freetuple](../h/heap_freetuple.md) (function)
- Called from (representative examples):
  - [index_create](index_create.md)
  - [index_build](index_build.md)

## Notes and Other Information
- Uses non-transactional updates to handle concurrent CREATE INDEX operations safely
- Special handling prevents empty tables from appearing vacuumed during CREATE TABLE
- Skips statistics updates during binary upgrade mode to avoid inconsistencies
- Always triggers relcache invalidation even if no changes are made to ensure proper cache coherency
- Handles both regular relations and indexes, with different processing for visibility map statistics
- The function's "bizarre API" is specifically designed to perform all necessary updates in a single operation
- Critical for maintaining catalog consistency during index operations and ensuring other backends are notified of changes