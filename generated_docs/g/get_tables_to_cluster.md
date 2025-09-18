# get_tables_to_cluster

## Location
src/backend/commands/cluster.c: 1636 - 1689

## Overview
Returns a list of tables that the current user has privileges on and have the indisclustered flag set, identifying tables that are ready for clustering operations.

## Definition
static List *get_tables_to_cluster(MemoryContext cluster_context)

## Detailed Description
This function scans the pg_index system catalog to find all indexes that have the indisclustered flag set to true, indicating that the associated table should be clustered using that index. For each such index found, the function verifies that the current user has appropriate privileges on the underlying table before including it in the result list. The function creates RelToCluster structures containing both the table OID and the index OID for each qualifying table-index pair.

The function performs a catalog scan on the IndexRelationId relation, filtering for entries where indisclustered is true. It then checks permissions using cluster_is_permitted_for_relation() before adding entries to the result list. All result structures are allocated in the specified memory context to ensure proper memory management.

## Parameters / Member Variables
- : Memory context in which to allocate the result list and RelToCluster structures

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - ScanKeyInit
  - table_beginscan_catalog
  - heap_getnext
  - cluster_is_permitted_for_relation
  - MemoryContextSwitchTo
  - palloc
  - lappend
  - table_endscan
  - relation_close
- Called from (representative examples):
  - cluster

## Notes and Other Information
- This is a static function internal to cluster.c
- Uses AccessShareLock when opening the index relation
- Scans the pg_index catalog with indisclustered = true filter
- Returns NIL if no clusterable tables are found or user lacks privileges
- Memory allocation is done in the specified cluster_context for proper cleanup
- Each RelToCluster structure contains both tableOid and indexOid for the clustering operation