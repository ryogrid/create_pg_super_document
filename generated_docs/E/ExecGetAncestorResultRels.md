# ExecGetAncestorResultRels

## Location
src/backend/executor/execMain.c: 1371 - 1430

## Overview
Returns ResultRelInfo structures for all ancestor relations of a given leaf partition up to and including the query's root target relation, enabling operations that must traverse the partition hierarchy.

## Definition


## Detailed Description
ExecGetAncestorResultRels builds and caches a list of ResultRelInfo structures representing the complete ancestry chain of a partition relation. It uses get_partition_ancestors to obtain the OID list of ancestor relations, then creates corresponding ResultRelInfo structures for each ancestor up to (but not including) the root relation mentioned in the query. The root relation is added separately using the existing ri_RootResultRelInfo. This functionality is essential for operations like foreign key constraint checking that must propagate across partition boundaries.

## Parameters / Member Variables
- : The execution state containing instrumentation and context information  
- : The leaf partition's ResultRelInfo for which ancestors are needed

## Dependencies
- Functions called/Symbols referenced:
  - [get_partition_ancestors](../g/get_partition_ancestors.md)
  - [InitResultRelInfo](../I/InitResultRelInfo.md)
  - RelationGetRelid
  - table_open
  - makeNode
  - lappend
  - elog/elog
  - Assert
- Called from (representative examples):
  - [ExecCrossPartitionUpdateForeignKey](ExecCrossPartitionUpdateForeignKey.md) (nodeModifyTable.c:2212)

## Notes and Other Information
- Only works with partition relations; errors out if called on non-partitioned relations
- Results are cached in ri_ancestorResultRels to avoid repeated computation
- Assumes all ancestor relations are properly locked by planner or AcquireExecutorLocks
- Opens ancestor relations with NoLock since locks should already be held
- Stops climbing the hierarchy when reaching the query's root target relation
- Always includes the root relation as the final element in the returned list
- Essential for maintaining referential integrity across partition boundaries
- Used by foreign key enforcement during cross-partition updates
- Closed automatically by ExecCloseResultRelations during cleanup