# get_tables_to_cluster_partitioned

## Location
src/backend/commands/cluster.c: 1690 - 1737

## Overview
Returns a list of RelToCluster structures for all leaf table/index pairs that should be clustered, given an index on a partitioned table.

## Definition
static List *get_tables_to_cluster_partitioned(MemoryContext cluster_context, Oid indexOid)

## Detailed Description
This function handles clustering operations on partitioned tables by expanding a partitioned index into all of its leaf partition indexes and their corresponding tables. It starts with a given index OID on a partitioned table and uses find_all_inheritors() to discover all child indexes. For each child index found, it verifies that it's actually a leaf index (not another partitioned index) and that the current user has clustering privileges on the corresponding table.

The function is similar to expand_vacuum_rel but is specifically designed for clustering operations. The caller must already hold AccessExclusiveLock on the table containing the index. The function skips any partitions where the user lacks CLUSTER privileges, allowing partial clustering when permissions are limited.

## Parameters / Member Variables
- : Memory context in which to allocate the result list and RelToCluster structures
- : OID of the partitioned index to expand into leaf indexes

## Dependencies
- Functions called/Symbols referenced:
  - find_all_inheritors
  - IndexGetRelation
  - get_rel_relkind
  - cluster_is_permitted_for_relation
  - MemoryContextSwitchTo
  - palloc
  - lappend
- Called from (representative examples):
  - cluster

## Notes and Other Information
- This is a static function internal to cluster.c
- Designed for partitioned table clustering operations
- Uses NoLock when calling find_all_inheritors since caller already holds AccessExclusiveLock
- Filters out non-leaf indexes using get_rel_relkind() check for RELKIND_INDEX
- Handles permission checking per partition, allowing partial clustering
- Returns NIL if no leaf partitions are found or user lacks privileges on all partitions
- Memory allocation is done in the specified cluster_context for proper cleanup