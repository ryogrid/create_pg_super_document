# ExecGetRangeTableRelation

## Location
src/backend/executor/execUtils.c: 762 - 813

## Overview
Opens a Relation for a range table entry if not already opened, providing lazy initialization of table access during query execution.

## Definition


## Detailed Description
This function implements lazy opening of relations referenced in the query's range table. It checks if the relation at the given range table index (rti) is already open in the execution state, and if not, opens it using the appropriate locking mechanism. The function handles both normal query execution and parallel worker scenarios differently - parallel workers must obtain their own local locks to ensure safe behavior if the parent process exits prematurely. All opened relations are stored in the execution state and will be automatically closed when the plan execution ends via ExecEndPlan().

## Parameters / Member Variables
- : Execution state containing the range table and opened relations array
- : Range table index (1-based) identifying which relation to open

## Dependencies
- Functions called/Symbols referenced:
  - exec_rt_fetch
  - table_open
  - IsParallelWorker
  - CheckRelationLockedByMe
- Called from (representative examples):
  - InitPlan
  - CreatePartitionPruneState
  - ExecOpenScanRelation
  - ExecInitResultRelation

## Notes and Other Information
- Relations are opened with NoLock in normal execution (assumes appropriate lock already held)
- Parallel workers explicitly acquire the lock specified in rte->rellockmode
- Uses lazy initialization pattern - relations are only opened when first accessed
- Includes assertion checks to verify proper locking in non-parallel execution
- The function assumes the range table entry is of type RTE_RELATION