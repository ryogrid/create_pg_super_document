# spg_redo

## Location
src/backend/access/spgist/spgxlog.c: 935 - 975

## Overview
The main entry point function for replaying SP-GiST (Space-Partitioned GiST) index Write-Ahead Log (WAL) records during PostgreSQL recovery operations.

## Definition


## Detailed Description
 is the central dispatcher function responsible for replaying SP-GiST index operations from WAL records during database recovery. It extracts the operation type from the WAL record and delegates to specific redo functions based on the operation type. The function operates within a dedicated memory context () to ensure proper memory management during recovery operations, switching to this context at the beginning and resetting it after processing to prevent memory leaks.

The function handles eight different types of SP-GiST operations:
- Adding leaf tuples
- Moving leaf tuples between pages
- Adding inner nodes
- Splitting tuples
- Pick-split operations (for node splitting decisions)
- Vacuuming leaf pages
- Vacuuming root pages  
- Vacuuming redirect tuples

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record to be replayed, including the operation type and associated data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo (extracts info byte from WAL record)
  - XLR_INFO_MASK (masks out non-operation bits)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (switches memory context)
  - [MemoryContextReset](../M/MemoryContextReset.md) (resets memory context)
  - [spgRedoAddLeaf](spgRedoAddLeaf.md) (handles XLOG_SPGIST_ADD_LEAF)
  - [spgRedoMoveLeafs](spgRedoMoveLeafs.md) (handles XLOG_SPGIST_MOVE_LEAFS)
  - [spgRedoAddNode](spgRedoAddNode.md) (handles XLOG_SPGIST_ADD_NODE)
  - [spgRedoSplitTuple](spgRedoSplitTuple.md) (handles XLOG_SPGIST_SPLIT_TUPLE)
  - [spgRedoPickSplit](spgRedoPickSplit.md) (handles XLOG_SPGIST_PICKSPLIT)
  - [spgRedoVacuumLeaf](spgRedoVacuumLeaf.md) (handles XLOG_SPGIST_VACUUM_LEAF)
  - [spgRedoVacuumRoot](spgRedoVacuumRoot.md) (handles XLOG_SPGIST_VACUUM_ROOT)
  - [spgRedoVacuumRedirect](spgRedoVacuumRedirect.md) (handles XLOG_SPGIST_VACUUM_REDIRECT)
  - elog (error logging with PANIC level for unknown operations)
- Called from (representative examples):
  - SizeOfSpgxlogVacuumRedirect (referenced in spgxlog.h)

## Notes and Other Information
- This function is critical for crash recovery and replication in PostgreSQL SP-GiST indexes
- Uses a dedicated memory context () that is reset after each operation to prevent memory accumulation during recovery
- Will panic the system if an unknown operation code is encountered, ensuring data consistency
- Located in src/backend/access/spgist/spgxlog.c:935-975
- Part of the SP-GiST access method's WAL replay infrastructure