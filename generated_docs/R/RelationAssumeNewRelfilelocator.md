# RelationAssumeNewRelfilelocator

## Location
src/backend/utils/cache/relcache.c: 3971 - 3993

## Overview
RelationAssumeNewRelfilelocator notifies the relation cache that a relation has received a new RelFileLocator, ensuring proper WAL ordering and transaction cleanup tracking.

## Definition


## Detailed Description
This function is a critical component of PostgreSQL's relation file management system that must be called whenever code modifies pg_class.reltablespace or pg_class.relfilenode. Its primary purpose is to maintain proper WAL (Write-Ahead Log) ordering constraints and ensure transaction cleanup occurs correctly.

The function performs two essential tracking operations:

1. **SubTransaction Tracking**: Records the current subtransaction ID in rd_newRelfilelocatorSubid to track when the RelFileLocator change occurred. If this is the first RelFileLocator change for the relation in the current transaction tree, it also sets rd_firstRelfilelocatorSubid.

2. **Cleanup Registration**: Adds the relation to the end-of-transaction cleanup list via EOXactListAdd(), ensuring the tracking fields are properly cleared when the transaction completes.

The timing of this call is crucial for WAL consistency. It must be positioned carefully in relation to WAL record insertions to ensure that WAL records modifying the old RelFileLocator are written before this call, and WAL records modifying the new RelFileLocator are written after this call. This ordering prevents replay inconsistencies during crash recovery.

## Parameters / Member Variables
- : The relation that has assumed a new RelFileLocator (tablespace and/or relfilenode change)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md)
  - EOXactListAdd
- Called from (representative examples):
  - [RelationSetNewRelfilenumber](RelationSetNewRelfilenumber.md)
  - [reindex_index](../r/reindex_index.md)
  - [swap_relation_files](../s/swap_relation_files.md)
  - [ATExecSetTableSpace](../A/ATExecSetTableSpace.md)

## Notes and Other Information
- Must be called for any code that modifies pg_class.reltablespace or pg_class.relfilenode
- Call timing is critical: must precede WAL records for new RelFileLocator, must follow WAL records for old RelFileLocator
- Should be called as close as possible to the CommandCounterIncrement() that makes the pg_class change visible
- The function ensures proper cleanup of tracking fields at transaction end
- Essential for maintaining WAL replay consistency during crash recovery
- The rd_firstRelfilelocatorSubid field tracks the very first RelFileLocator change in a transaction tree, while rd_newRelfilelocatorSubid tracks the most recent change
- Failure to call this function when required can lead to WAL replay issues and data corruption