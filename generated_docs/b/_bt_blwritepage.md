# _bt_blwritepage

## Location
[src/backend/access/nbtree/nbtsort.c:635-645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L635-L645)

## Overview
Emits a completed B-tree page to storage and releases the associated working buffer during bulk index construction.

## Definition
```c
static void _bt_blwritepage(BTWriteState *wstate, BulkWriteBuffer buf, BlockNumber blkno)
```

## Detailed Description
This function serves as the final step in the B-tree page creation process during bulk loading. It takes a completed page buffer and writes it to the specified block number using the bulk write interface. The function is a thin wrapper around smgr_bulk_write that handles the transfer of ownership of the buffer to the storage manager. Once called, the buffer is no longer owned by the caller and should not be accessed further.

## Parameters / Member Variables
- `wstate`: Pointer to BTWriteState structure containing bulk write context and state information
- `buf`: BulkWriteBuffer containing the completed page data to be written
- `blkno`: BlockNumber specifying the target block location in the index file

## Dependencies
- Functions called/Symbols referenced:
  - [BTWriteState](../B/BTWriteState.md) (parameter type)
  - BulkWriteBuffer (parameter type)
  - [smgr_bulk_write](../s/smgr_bulk_write.md) (to perform the actual write operation)
  - [BTPageState](../B/BTPageState.md) (referenced in context)
- Called from (representative examples):
  - [_bt_buildadd](_bt_buildadd.md)
  - [_bt_uppershutdown](_bt_uppershutdown.md)

## Notes and Other Information
- This is a static function, only accessible within the nbtsort.c compilation unit
- The function transfers ownership of the buffer to smgr_bulk_write, meaning the caller must not access the buffer after this call
- Part of the bulk loading optimization that batches writes for better I/O performance
- The 'true' parameter passed to smgr_bulk_write likely indicates that the write should be performed immediately or marked as ready
- Simple wrapper function that provides a clean interface for the bulk loading subsystem
- Critical for maintaining proper memory management during index construction