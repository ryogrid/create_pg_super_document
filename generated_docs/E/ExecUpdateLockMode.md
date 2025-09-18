# ExecUpdateLockMode

## Location
src/backend/executor/execMain.c: 2353 - 2378

## Overview
Determines the appropriate tuple lock mode for UPDATE operations based on whether key columns are being modified, enabling better concurrency when only non-key columns are updated.

## Definition
LockTupleMode ExecUpdateLockMode(EState *estate, ResultRelInfo *relinfo)

## Detailed Description
ExecUpdateLockMode analyzes an UPDATE operation to determine whether it modifies any columns that are part of a key (primary key or unique constraint). Based on this analysis, it returns the appropriate lock mode:
- If key columns are modified, returns LockTupleExclusive (stronger lock) to ensure data integrity
- If only non-key columns are modified, returns LockTupleNoKeyExclusive (weaker lock) to allow better concurrency

The function works by getting the bitmap of all updated columns and comparing it with the bitmap of key columns from all indexes on the relation. If there's any overlap, a stronger lock is required.

## Parameters / Member Variables
- `estate`: Execution state containing query context and runtime information
- `relinfo`: ResultRelInfo structure containing information about the target relation being updated

## Dependencies
- Functions called/Symbols referenced:
  - ExecGetAllUpdatedCols
  - RelationGetIndexAttrBitmap
  - INDEX_ATTR_BITMAP_KEY
  - bms_overlap
  - LockTupleExclusive
  - LockTupleNoKeyExclusive
- Called from (representative examples):
  - ExecBRUpdateTriggersNew
  - ExecOnConflictUpdate
  - ExecMergeMatched

## Notes and Other Information
This function is part of PostgreSQL's optimized locking strategy introduced to improve concurrency for UPDATE operations. When only non-key columns are updated, the weaker LockTupleNoKeyExclusive lock allows concurrent transactions to acquire shared locks on the same tuple, whereas LockTupleExclusive would block all concurrent access. This optimization is particularly beneficial for workloads with frequent updates to non-key columns.