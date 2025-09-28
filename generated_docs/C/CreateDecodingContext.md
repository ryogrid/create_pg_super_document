# CreateDecodingContext

## Location
[src/backend/replication/logical/logical.c:498-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L498-L642)

## Overview
CreateDecodingContext creates a logical decoding context for an existing logical replication slot that has been previously used, handling slot validation and restart position management.

## Definition

```c
LogicalDecodingContext *
CreateDecodingContext(XLogRecPtr start_lsn,
					  List *output_plugin_options,
					  bool fast_forward,
					  XLogReaderRoutine *xl_routine,
					  LogicalOutputPluginWriterPrepareWrite prepare_write,
					  LogicalOutputPluginWriterWrite do_write,
					  LogicalOutputPluginWriterUpdateProgress update_progress)
```
## Detailed Description
This function initializes a logical decoding context for resuming logical replication from an existing slot. Unlike CreateInitDecodingContext, it works with pre-configured slots and handles restart position logic, slot validation, and various error conditions that can occur with established slots.

Key operations include:
1. Comprehensive slot validation (existence, type, database, synchronization status)
2. Slot invalidation checking (WAL removal, recovery conflicts)
3. Start position resolution (uses confirmed_flush if start_lsn is invalid)
4. LSN adjustment handling (forwards to confirmed_flush if requested LSN is too old)
5. Decoding context startup via StartupDecodingContext()
6. Output plugin startup callback invocation
7. Two-phase commit configuration and slot metadata updates
8. Progress logging for decoding startup

The function includes sophisticated error handling for various slot states including invalidated slots, synchronized slots on standbys, and database mismatches.

## Parameters / Member Variables
- : WAL position to start decoding from (InvalidXLogRecPtr for auto-selection)
- : Options to pass to the output plugin
- : Skip change generation for fast position advancement
- : WAL reading routine function pointer
- : Callback for preparing output buffer writes
- : Callback for performing actual output writes
- : Callback for progress reporting during decoding

## Dependencies
- Functions called/Symbols referenced:
  - SlotIsPhysical: Validates slot is logical type
  - [RecoveryInProgress](../R/RecoveryInProgress.md): Checks if server is in recovery mode
  - [IsSyncingReplicationSlots](../I/IsSyncingReplicationSlots.md): Checks if slot synchronization is active
  - [StartupDecodingContext](../S/StartupDecodingContext.md): Common decoding context initialization
  - [startup_cb_wrapper](../s/startup_cb_wrapper.md): Output plugin startup callback wrapper
  - [ReplicationSlotMarkDirty](../R/ReplicationSlotMarkDirty.md)/ReplicationSlotSave: Slot persistence operations
  - [SnapBuildSetTwoPhaseAt](../S/SnapBuildSetTwoPhaseAt.md): Configures two-phase snapshot building

- Called from (representative examples):
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md): During SQL function change retrieval
  - [StartLogicalReplication](../S/StartLogicalReplication.md): During WAL sender logical replication startup
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md): During slot position advancement

## Notes and Other Information
- Handles graceful LSN adjustment when requested position is behind confirmed_flush
- Includes comprehensive slot invalidation detection and error reporting
- Supports two-phase commit with dynamic slot metadata updates
- Fast-forward mode bypasses database validation for performance
- Synchronized slots on standby servers are restricted to synchronization operations only
- Logs detailed startup information including streaming and restart positions
- Critical for resuming logical replication from existing, established slots

## Simplified Source

```c
// Simplified version of CreateDecodingContext
LogicalDecodingContext *CreateDecodingContext(XLogRecPtr start_lsn,
                                               List *output_plugin_options,
                                               bool fast_forward,
                                               XLogReaderRoutine *xl_routine,
                                               LogicalOutputPluginWriterPrepareWrite prepare_write,
                                               LogicalOutputPluginWriterWrite do_write,
                                               LogicalOutputPluginWriterUpdateProgress update_progress) {
    LogicalDecodingContext *ctx;
    ReplicationSlot *slot;
    MemoryContext old_context;

    slot = MyReplicationSlot;

    // Basic slot validation
    if (slot == NULL)
        elog(ERROR, "cannot perform logical decoding without an acquired slot");

    if (SlotIsPhysical(slot))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("cannot use physical replication slot for logical decoding")));

    // Database validation (except in fast_forward mode)
    if (slot->data.database != MyDatabaseId && !fast_forward)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("replication slot \"%s\" was not created in this database",
                             NameStr(slot->data.name))));

    // Check for synchronized slots on standby
    if (RecoveryInProgress() && slot->data.synced && !IsSyncingReplicationSlots())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("cannot use replication slot \"%s\" for logical decoding",
                             NameStr(slot->data.name)),
                       errdetail("This replication slot is being synchronized from the primary server.")));

    // Check slot invalidation
    if (MyReplicationSlot->data.invalidated == RS_INVAL_WAL_REMOVED)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("can no longer get changes from replication slot \"%s\"",
                             NameStr(MyReplicationSlot->data.name)),
                       errdetail("This slot has been invalidated because it exceeded the maximum reserved size.")));

    if (MyReplicationSlot->data.invalidated != RS_INVAL_NONE)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("can no longer get changes from replication slot \"%s\"",
                             NameStr(MyReplicationSlot->data.name))));

    // Determine start position
    if (start_lsn == InvalidXLogRecPtr) {
        start_lsn = slot->data.confirmed_flush;  // Continue from last position
    } else if (start_lsn < slot->data.confirmed_flush) {
        // Forward to confirmed_flush if requested LSN is too old
        elog(LOG, "%X/%X has been already streamed, forwarding to %X/%X",
             LSN_FORMAT_ARGS(start_lsn), LSN_FORMAT_ARGS(slot->data.confirmed_flush));
        start_lsn = slot->data.confirmed_flush;
    }

    // Create the decoding context
    ctx = StartupDecodingContext(output_plugin_options, start_lsn, InvalidTransactionId,
                                false, fast_forward, false, xl_routine,
                                prepare_write, do_write, update_progress);

    // Initialize output plugin
    old_context = MemoryContextSwitchTo(ctx->context);
    if (ctx->callbacks.startup_cb != NULL)
        startup_cb_wrapper(ctx, &ctx->options, false);
    MemoryContextSwitchTo(old_context);

    // Handle two-phase commit configuration
    ctx->twophase &= (slot->data.two_phase || ctx->twophase_opt_given);
    if (ctx->twophase && !slot->data.two_phase) {
        SpinLockAcquire(&slot->mutex);
        slot->data.two_phase = true;
        slot->data.two_phase_at = start_lsn;
        SpinLockRelease(&slot->mutex);
        ReplicationSlotMarkDirty();
        ReplicationSlotSave();
        SnapBuildSetTwoPhaseAt(ctx->snapshot_builder, start_lsn);
    }

    ctx->reorder->output_rewrites = ctx->options.receive_rewrites;

    // Log startup information
    ereport(LOG, (errmsg("starting logical decoding for slot \"%s\"", NameStr(slot->data.name)),
                  errdetail("Streaming transactions committing after %X/%X, reading WAL from %X/%X.",
                           LSN_FORMAT_ARGS(slot->data.confirmed_flush),
                           LSN_FORMAT_ARGS(slot->data.restart_lsn))));

    return ctx;
}
```

Key simplifications made:
- Condensed multiple validation checks while preserving all essential error conditions
- Simplified LSN position resolution logic with clear comments
- Maintained all critical slot invalidation and synchronization checks
- Preserved two-phase commit configuration logic
- Retained comprehensive error messages for troubleshooting