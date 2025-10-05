# _bt_parallel_estimate_shared

## Location
[src/backend/access/nbtree/nbtsort.c:1633-1652](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L1633-L1652)

## Overview
Calculates the total shared memory size required for B-tree parallel index build operations, including both B-tree specific state and parallel table scan requirements.

## Definition

```c
static Size
_bt_parallel_estimate_shared(Relation heap, Snapshot snapshot)
```
## Detailed Description
This function provides a memory estimation for the shared memory segment that will be used during parallel B-tree index construction. It combines the memory needed for B-tree specific shared state (BTShared structure) with the memory required for parallel table scanning operations.

The function ensures proper memory alignment using BUFFERALIGN, which is critical for shared memory structures that may be accessed by multiple processes with different alignment requirements. The estimation includes:
- BTShared structure containing index metadata, coordination variables, and build statistics
- Parallel table scan state managed by the table access method layer

This estimation is used by the parallel context setup code to allocate an appropriately sized dynamic shared memory segment before launching worker processes.

## Parameters / Member Variables
- `heap`: Relation being scanned to build the index (the base table)
- `snapshot`: Snapshot that will be used for the parallel scan (affects scan state size)
## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md): Safe arithmetic function for adding Size values
  - BUFFERALIGN: Macro for buffer alignment requirements
  - [table_parallelscan_estimate](../t/table_parallelscan_estimate.md): Estimate memory for parallel table scan state
  - [BTShared](../B/BTShared.md): B-tree specific shared state structure
- Called from (representative examples):
  - [_bt_begin_parallel](_bt_begin_parallel.md): Main parallel setup function that uses this estimate

## Notes and Other Information
- Uses BUFFERALIGN to ensure proper memory alignment for shared structures
- The total estimate includes both B-tree specific and table access method requirements
- [Snapshot](../S/Snapshot.md) type affects the parallel scan estimation (MVCC vs SnapshotAny)
- Critical for preventing shared memory allocation failures during parallel setup
- Memory estimation must be accurate to avoid runtime allocation errors

## Simplified Source

```c
static Size
_bt_parallel_estimate_shared(Relation heap, Snapshot snapshot)
{
    // Calculate total shared memory needed:
    // - BTShared structure (aligned)
    // - Parallel table scan state
    return add_size(BUFFERALIGN(sizeof(BTShared)),
                    table_parallelscan_estimate(heap, snapshot));
}
```