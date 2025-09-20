# ReorderBufferCopySnap

## Location
[src/backend/replication/logical/reorderbuffer.c:1851-1909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1851-L1909)

## Overview
Creates a private copy of a snapshot that can be modified for catalog access, enabling logical decoding to examine intermediate catalog states during transaction processing.

## Definition

```c
static Snapshot
ReorderBufferCopySnap(ReorderBuffer *rb, Snapshot orig_snap,
					  ReorderBufferTXN *txn, CommandId cid)
```
## Detailed Description
This function creates a specialized copy of a PostgreSQL snapshot that is tailored for logical decoding operations. The copied snapshot allows catalog-modifying transactions to examine intermediate catalog states by incorporating transaction-specific information.

The function performs several critical operations:

1. **Size calculation**: Computes the total memory needed for the snapshot copy, including space for the original transaction IDs (xcnt) and additional space for the transaction and all its subtransactions.

2. **Memory allocation and copying**: Allocates memory in the reorder buffer's context and copies the base snapshot data, then sets up the copied snapshot with appropriate reference counts and flags.

3. **Transaction ID array setup**: Sets up two arrays within the snapshot:
   - : Contains the original transaction IDs from the base snapshot
   - : Contains transaction IDs that need special cmin/cmax checking

4. **Subtransaction processing**: Populates the subxip array with:
   - The toplevel transaction ID (always included)
   - All subtransaction IDs from the transaction's subtxn list
   - Manual counting of active subtransactions (since subxcnt may include aborted ones)

5. **Array optimization**: Sorts the subxip array to enable efficient binary search operations during snapshot visibility checks.

6. **Command ID assignment**: Sets the snapshot's current command ID (curcid) to the specified value, enabling proper visibility determination for catalog access.

The resulting snapshot maintains all the properties of the original while incorporating transaction-specific visibility rules necessary for logical decoding operations.

## Parameters / Member Variables
- : Pointer to the ReorderBuffer structure providing the memory context for snapshot allocation
- : The original snapshot to be copied and modified
- : Pointer to the ReorderBufferTXN structure whose transaction and subtransaction IDs will be incorporated into the snapshot
- : The CommandId to set as the current command ID in the copied snapshot

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - memcpy
  - dlist_foreach
  - dlist_container
  - qsort
  - [xidComparator](../x/xidComparator.md)
- Called from (representative examples):
  - [ReorderBufferSaveTXNSnapshot](ReorderBufferSaveTXNSnapshot.md)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md) (multiple calls)
  - [ReorderBufferStreamTXN](ReorderBufferStreamTXN.md) (multiple calls)

## Notes and Other Information
- This is a static function, accessible only within reorderbuffer.c
- The copied snapshot is marked with copied=true and active_count=1 to prevent premature deallocation
- Memory layout is optimized with xip and subxip arrays placed consecutively after the snapshot header
- The function handles the case where subtransaction counts may be inaccurate due to aborted subtransactions
- Binary search optimization through qsort improves performance for large transaction hierarchies
- The snapshot lifetime is managed by the reorder buffer's memory context
- Essential for maintaining MVCC consistency during logical decoding operations
- Part of PostgreSQL's logical replication infrastructure for catalog access during transaction replay