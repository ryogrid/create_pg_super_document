# ParallelBlockTableScanDescData

## Location
[src/include/access/relscan.h:75-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/relscan.h#L75-L84)

## Overview
ParallelBlockTableScanDescData extends ParallelTableScanDescData to provide shared state coordination specifically for block-oriented storage parallel table scans.

## Definition
```c
typedef struct ParallelBlockTableScanDescData
{
    ParallelTableScanDescData base;

    BlockNumber phs_nblocks;        /* # blocks in relation at start of scan */
    slock_t     phs_mutex;          /* mutual exclusion for setting startblock */
    BlockNumber phs_startblock;     /* starting block number */
    pg_atomic_uint64 phs_nallocated; /* number of blocks allocated to
                                      * workers so far. */
} ParallelBlockTableScanDescData;
```

## Detailed Description
ParallelBlockTableScanDescData is a specialized structure for coordinating parallel table scans on block-oriented storage systems (such as heap tables). It extends the base ParallelTableScanDescData structure with additional fields specific to block-level coordination. This structure manages the distribution of blocks among parallel worker processes, ensuring that each block is scanned exactly once while minimizing coordination overhead. The structure uses atomic operations and lightweight locking to efficiently coordinate block allocation among workers.

## Parameters / Member Variables
- `base`: Base ParallelTableScanDescData structure containing common parallel scan state
- `phs_nblocks`: Total number of blocks in the relation at the start of the scan, establishing the scan boundary
- `phs_mutex`: Spinlock for mutual exclusion when updating the starting block number or other shared state
- `phs_startblock`: The block number from which scanning should begin, allowing for coordinated scan start points
- `phs_nallocated`: Atomic counter tracking the total number of blocks that have been allocated to worker processes so far

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelTableScanDescData](ParallelTableScanDescData.md)
  - [slock_t](../s/slock_t.md)
  - [pg_atomic_uint64](../p/pg_atomic_uint64.md)
- Called from (representative examples):
  - [table_block_parallelscan_estimate](../t/table_block_parallelscan_estimate.md) (src/backend/access/table/tableam.c:385)
  - [table_block_parallelscan_initialize](../t/table_block_parallelscan_initialize.md) (src/backend/access/table/tableam.c:403)
  - [ParallelBlockTableScanDesc](ParallelBlockTableScanDesc.md) (src/include/access/relscan.h:85)

## Notes and Other Information
This structure is defined in src/include/access/relscan.h (lines 75-84) and provides the foundation for efficient parallel scanning of block-oriented tables. The design uses a combination of atomic operations (phs_nallocated) and lightweight locking (phs_mutex) to minimize contention while ensuring correct block distribution. The block allocation strategy helps balance the workload among parallel workers while maintaining good locality of reference. This structure is typically used with heap tables and other block-oriented access methods that can benefit from parallel block-level scanning.