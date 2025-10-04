# LogicalSlotAdvanceAndCheckSnapState

## Location
[src/backend/replication/logical/logical.c:2108-2223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L2108-L2223)

## Overview
Helper function that advances a logical replication slot forward to a specified LSN position while maintaining proper snapshot state and allowing WAL recycling.

## Definition
```c
XLogRecPtr LogicalSlotAdvanceAndCheckSnapState(XLogRecPtr moveto, bool *found_consistent_snapshot)
```

## Detailed Description
This function advances a logical replication slot by reading and processing WAL records from the slot's restart_lsn up to the specified target LSN (`moveto`). The advancement is done in fast_forward mode, meaning no actual logical changes are decoded or output, but the slot's internal state (including snapshot building) is properly maintained.

The function serves as a critical component for logical replication slot management, allowing slots to advance their position without generating decoded changes. This is essential for:
- Preventing WAL accumulation by advancing restart_lsn
- Allowing removal of old catalog tuples
- Building initial snapshots for consistent decoding
- Maintaining slot state consistency

The operation is performed within a PG_TRY/PG_CATCH block to ensure proper cleanup of system caches in case of errors.

## Parameters / Member Variables
- `moveto`: Target XLogRecPtr to advance the slot to. Must be a valid LSN (not InvalidXLogRecPtr)
- `found_consistent_snapshot`: Output parameter that indicates whether an initial consistent snapshot has been built during the advancement process

## Dependencies
- Functions called/Symbols referenced:
  - [CreateDecodingContext](../C/CreateDecodingContext.md) - Creates logical decoding context in fast_forward mode
  - [WaitForStandbyConfirmation](../W/WaitForStandbyConfirmation.md) - Waits for standby servers to confirm WAL receipt
  - [XLogBeginRead](../X/XLogBeginRead.md) - Begins reading from slot's restart_lsn
  - [XLogReadRecord](../X/XLogReadRecord.md) - Reads individual WAL records
  - [LogicalDecodingProcessRecord](LogicalDecodingProcessRecord.md) - Processes records for snapshot building
  - [DecodingContextReady](../D/DecodingContextReady.md) - Checks if decoding context has consistent snapshot
  - [LogicalConfirmReceivedLocation](LogicalConfirmReceivedLocation.md) - Updates slot's confirmed_flush position
  - [ReplicationSlotMarkDirty](../R/ReplicationSlotMarkDirty.md) - Marks slot for checkpoint writing
  - [FreeDecodingContext](../F/FreeDecodingContext.md) - Cleans up decoding context
  - [InvalidateSystemCaches](../I/InvalidateSystemCaches.md) - Invalidates cached catalog information

- Called from (representative examples):
  - [update_local_synced_slot](../u/update_local_synced_slot.md) - Updates synchronized replication slots
  - [pg_logical_replication_slot_advance](../p/pg_logical_replication_slot_advance.md) - SQL interface for slot advancement

## Notes and Other Information
- The function uses fast_forward mode to avoid generating actual decoded changes while still maintaining internal state
- System caches are invalidated before and after processing to ensure catalog consistency
- Resource owner is preserved and restored to handle transaction management side effects
- The slot is marked dirty after advancement to ensure persistence at next checkpoint
- Error handling ensures proper cleanup of system caches even in failure cases
- The function is essential for slot synchronization and SQL-interface slot management
- Located in src/backend/replication/logical/logical.c at lines 2108-2223

## Simplified Source

```c
XLogRecPtr LogicalSlotAdvanceAndCheckSnapState(XLogRecPtr moveto,
                                               bool *found_consistent_snapshot)
{
    LogicalDecodingContext *ctx;
    ResourceOwner old_resowner = CurrentResourceOwner;
    XLogRecPtr retlsn;

    Assert(moveto != InvalidXLogRecPtr);

    if (found_consistent_snapshot)
        *found_consistent_snapshot = false;

    PG_TRY();
    {
        // Create decoding context in fast_forward mode
        ctx = CreateDecodingContext(InvalidXLogRecPtr,  // start from confirmed_flush
                                    NIL,
                                    true,  // fast_forward mode
                                    XL_ROUTINE(.page_read = read_local_xlog_page,
                                               .segment_open = wal_segment_open,
                                               .segment_close = wal_segment_close),
                                    NULL, NULL, NULL);

        // Wait for standby confirmation
        WaitForStandbyConfirmation(moveto);

        // Start reading from slot's restart_lsn
        XLogBeginRead(ctx->reader, MyReplicationSlot->data.restart_lsn);

        InvalidateSystemCaches();

        // Process records until target LSN reached
        while (ctx->reader->EndRecPtr < moveto)
        {
            char *errm = NULL;
            XLogRecord *record;

            // Read WAL record
            record = XLogReadRecord(ctx->reader, &errm);
            if (errm)
                elog(ERROR, "could not find record while advancing replication slot: %s", errm);

            // Process record for snapshot building (no changes generated in fast_forward)
            if (record)
                LogicalDecodingProcessRecord(ctx, ctx->reader);

            CHECK_FOR_INTERRUPTS();
        }

        // Check if consistent snapshot was built
        if (found_consistent_snapshot && DecodingContextReady(ctx))
            *found_consistent_snapshot = true;

        // Restore resource owner
        CurrentResourceOwner = old_resowner;

        // Update slot position and mark dirty
        if (ctx->reader->EndRecPtr != InvalidXLogRecPtr)
        {
            LogicalConfirmReceivedLocation(moveto);
            ReplicationSlotMarkDirty();  // Ensure persistence at checkpoint
        }

        retlsn = MyReplicationSlot->data.confirmed_flush;

        // Cleanup
        FreeDecodingContext(ctx);
        InvalidateSystemCaches();
    }
    PG_CATCH();
    {
        InvalidateSystemCaches();
        PG_RE_THROW();
    }
    PG_END_TRY();

    return retlsn;
}
```