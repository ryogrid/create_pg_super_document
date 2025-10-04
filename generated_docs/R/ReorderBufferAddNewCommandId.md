# ReorderBufferAddNewCommandId

## Location
[src/backend/replication/logical/reorderbuffer.c:3232-3259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3232-L3259)

## Overview
ReorderBufferAddNewCommandId registers a new CommandId in the reorder buffer's change stream to ensure proper catalog access timing during logical decoding.

## Definition
```c
void ReorderBufferAddNewCommandId(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn, CommandId cid)
```

## Detailed Description
This function creates an internal change entry that marks when a new CommandId becomes active within a transaction's change stream. CommandIds are used in PostgreSQL to track the visibility of catalog changes within a single transaction - each SQL command within a transaction gets its own CommandId, and catalog changes made by a command are only visible to subsequent commands with higher CommandIds.

For logical decoding, it's crucial to track when CommandIds change so that catalog lookups use the correct visibility rules. This function queues an internal command ID change that will be processed at the appropriate LSN, ensuring that subsequent changes in the stream will see the catalog state as it existed at that point in the transaction.

The function is restricted to CommandIds greater than 1, as indicated by the comment, suggesting that the initial command (CommandId 1) is handled differently or assumed to be the baseline.

## Parameters
- `rb`: Pointer to the ReorderBuffer instance managing the transaction
- `xid`: The TransactionId to which this CommandId belongs
- `lsn`: The Log Sequence Number where this CommandId becomes active
- `cid`: The CommandId that should be used for catalog access from this point forward (must be > 1)

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferGetChange](ReorderBufferGetChange.md)
  - [ReorderBufferQueueChange](ReorderBufferQueueChange.md)
  - REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID
- Called from (representative examples):
  - [SnapBuildProcessNewCid](../S/SnapBuildProcessNewCid.md)

## Notes and Other Information
- May only be called for CommandIds greater than 1, as noted in the function comment
- The change is marked with action type REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID to distinguish it from data changes
- This is essential for maintaining correct catalog visibility during complex transactions with multiple SQL commands
- The change is queued with `false` as the last parameter, indicating this is not a top-level change
- CommandIds are fundamental to PostgreSQL's MVCC system and transaction isolation

## Simplified Source

```c
void ReorderBufferAddNewCommandId(ReorderBuffer *rb, TransactionId xid,
                                 XLogRecPtr lsn, CommandId cid)
{
    // Allocate a new change structure
    ReorderBufferChange *change = ReorderBufferGetChange(rb);

    // Set up the change as an internal command ID change
    change->data.command_id = cid;
    change->action = REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID;

    // Queue the change to be processed at the appropriate LSN
    ReorderBufferQueueChange(rb, xid, lsn, change, false);
}
```