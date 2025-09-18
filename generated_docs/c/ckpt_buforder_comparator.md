# ckpt_buforder_comparator

## Location
src/backend/storage/buffer/bufmgr.c: 5823 - 5853

## Overview
A static inline comparison function that determines the writeout order of dirty buffers during checkpoint operations, ensuring optimal I/O performance by organizing writes by tablespace, relation, fork, and block number.

## Definition
```c
static inline int ckpt_buforder_comparator(const CkptSortItem *a, const CkptSortItem *b)
```

## Detailed Description
The ckpt_buforder_comparator function establishes the ordering for writing dirty buffers to disk during checkpoint operations. This comparator is crucial for checkpoint performance as it determines the sequence in which pages are written to storage.

The comparison follows a strict hierarchical order:
1. Tablespace ID (tsId) - highest priority for load balancing across tablespaces
2. Relation number (relNumber) - groups pages by relation
3. Fork number (forkNum) - organizes by fork type within relations
4. Block number (blockNum) - ensures sequential access within forks

This ordering strategy optimizes I/O performance by:
- Balancing writes across different tablespaces/storage devices
- Minimizing seek times by writing related pages sequentially
- Reducing fragmentation and improving disk access patterns

## Parameters / Member Variables
- `a`: Pointer to the first CkptSortItem to compare
- `b`: Pointer to the second CkptSortItem to compare

## Dependencies
- Functions called/Symbols referenced:
  - CkptSortItem (type)
- Called from (representative examples):
  - BufferIsPinned (checkpoint sorting operations)

## Notes and Other Information
- The tablespace comparison is prioritized first to enable load balancing logic between tablespaces
- This function is performance-critical as it's used extensively during checkpoint sorting
- The hierarchical ordering ensures predictable and efficient write patterns during checkpoints
- Equal page IDs are rare but possible, handled by returning 0
- Used primarily in qsort operations to organize the checkpoint write order