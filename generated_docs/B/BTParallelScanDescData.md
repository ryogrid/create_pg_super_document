# BTParallelScanDescData

## Location
src/backend/access/nbtree/nbtree.c: 67 - 81

## Overview
BTParallelScanDescData is a structure that contains btree-specific shared information required for coordinating parallel scans of B-tree indexes in PostgreSQL.

## Definition


## Detailed Description
This structure serves as the shared state manager for parallel B-tree index scans in PostgreSQL. It coordinates multiple worker processes scanning a B-tree index simultaneously by tracking the current scan position, managing access synchronization, and maintaining state information needed for array key processing.

The structure uses a state machine approach (via btps_pageStatus) to coordinate workers, ensuring that only one process advances the scan at a time while others wait or work on their assigned pages. The flexible array member btps_arrElems supports complex scan scenarios involving array keys where multiple primitive scans may need to be scheduled.

## Parameters / Member Variables
- : BlockNumber indicating the latest page that has been scanned or the next page to be scanned by worker processes
- : BTPS_State enum value indicating the current state of the parallel scan (NOT_INITIALIZED, NEED_PRIMSCAN, ADVANCING, IDLE, or DONE)
- : Spinlock (slock_t) that protects access to the scan state variables and the btps_arrElems array
- : ConditionVariable used to synchronize worker processes during parallel scan operations
- : Flexible array member containing BTArrayKeyInfo.cur_elem offsets for scan keys, used when scheduling additional primitive index scans

## Dependencies
- Functions called/Symbols referenced:
  - BTPS_State (enum for parallel scan states)
  - [slock_t](../s/slock_t.md) (spinlock type)
  - ConditionVariable (synchronization primitive)
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array members)
  - BlockNumber (block identifier type)
- Called from (representative examples):
  - [BTParallelScanDesc](BTParallelScanDesc.md) (pointer typedef to this struct)
  - [btestimateparallelscan](../b/btestimateparallelscan.md) (for estimating parallel scan resources)

## Notes and Other Information
The structure is designed to be allocated in shared memory and accessed by multiple worker processes. The BTPS_State enum values represent different phases of the parallel scan lifecycle, with proper state transitions managed through the mutex and condition variable. The flexible array member allows the structure size to be determined at runtime based on the number of array elements needed for the specific scan operation.