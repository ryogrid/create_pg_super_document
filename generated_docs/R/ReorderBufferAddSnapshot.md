# ReorderBufferAddSnapshot

## Location
src/backend/replication/logical/reorderbuffer.c: 3183 - 3200

## Overview
ReorderBufferAddSnapshot adds a new snapshot to a transaction in the reorder buffer, ensuring catalog visibility is correctly maintained for rows processed after a specific LSN.

## Definition
```c
void ReorderBufferAddSnapshot(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn, Snapshot snap)
```

## Detailed Description
This function creates and queues an internal snapshot change within the reorder buffer for a specific transaction. The snapshot is necessary because the previous snapshot may not correctly describe the catalog state for subsequent rows that will be processed. This ensures that logical decoding has the proper visibility rules when interpreting changes that occur after the specified LSN.

The function allocates a new ReorderBufferChange structure, sets it up as an internal snapshot change, and queues it to be processed at the appropriate LSN within the transaction's change stream. This maintains the correct temporal ordering of catalog visibility changes relative to other transaction changes.

## Parameters
- `rb`: Pointer to the ReorderBuffer instance managing the transaction
- `xid`: The TransactionId to which this snapshot should be added
- `lsn`: The Log Sequence Number after which this snapshot becomes applicable
- `snap`: The Snapshot that provides the correct catalog visibility rules

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferGetChange
  - ReorderBufferQueueChange
  - REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT
- Called from (representative examples):
  - SnapBuildDistributeSnapshotAndInval

## Notes and Other Information
- The snapshot is marked with action type REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT to distinguish it from regular data changes
- This function is crucial for maintaining correct catalog visibility during logical decoding when catalog changes occur
- The snapshot will only be used for processing rows that come after the specified LSN
- The change is queued with `false` as the last parameter to ReorderBufferQueueChange, indicating this is not a top-level change