# CreateInitDecodingContext

## Location
[src/backend/replication/logical/logical.c:332-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L332-L497)

## Overview
CreateInitDecodingContext creates and initializes a new logical decoding context for a newly created logical replication slot, including complete setup of the slot's metadata and plugin initialization.

## Definition

```c
LogicalDecodingContext *
CreateInitDecodingContext(const char *plugin,
						  List *output_plugin_options,
						  bool need_full_snapshot,
						  XLogRecPtr restart_lsn,
						  XLogReaderRoutine *xl_routine,
						  LogicalOutputPluginWriterPrepareWrite prepare_write,
						  LogicalOutputPluginWriterWrite do_write,
						  LogicalOutputPluginWriterUpdateProgress update_progress)
```
## Detailed Description
This function performs comprehensive initialization of a logical decoding context for newly created slots. It validates prerequisites, configures the replication slot metadata, establishes transaction isolation boundaries, and initializes the output plugin.

Key operations include:
1. Prerequisites validation via CheckLogicalDecodingRequirements()
2. Slot validation (logical slot, correct database, no active writes)
3. Plugin name registration with the slot (thread-safe with spinlocks)
4. WAL reservation handling based on restart_lsn parameter
5. Safe transaction ID horizon calculation with ProcArrayLock protection
6. Slot xmin/catalog_xmin configuration for snapshot isolation
7. Startup of decoding context via StartupDecodingContext()
8. Output plugin startup callback invocation
9. Two-phase commit capability configuration

The function includes complex logic for determining safe decoding transaction IDs to prevent reading data that may have been vacuumed. It uses exclusive locks to ensure consistency during xmin horizon computation and slot metadata updates.

## Parameters / Member Variables
- `*plugin`: Name of the output plugin to load and initialize
- `*output_plugin_options`: Options to pass to the output plugin
- `need_full_snapshot`: Whether full table snapshot capability is required
- `restart_lsn`: WAL position to start from (InvalidXLogRecPtr for auto-selection)
- `*xl_routine`: WAL reading routine function pointer
- `prepare_write`: Callback for preparing output buffer writes
- `do_write`: Callback for performing actual output writes
- `update_progress`: Callback for progress reporting during decoding
## Dependencies
- Functions called/Symbols referenced:
  - [CheckLogicalDecodingRequirements](CheckLogicalDecodingRequirements.md): Validates decoding prerequisites
  - SlotIsPhysical: Checks if slot is physical type
  - [IsTransactionState](../I/IsTransactionState.md): Validates transaction state
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md): Gets active transaction ID if any
  - [ReplicationSlotReserveWal](../R/ReplicationSlotReserveWal.md): Reserves WAL for slot
  - [GetOldestSafeDecodingTransactionId](../G/GetOldestSafeDecodingTransactionId.md): Calculates safe decoding xmin
  - [ReplicationSlotsComputeRequiredXmin](../R/ReplicationSlotsComputeRequiredXmin.md): Updates global xmin requirements
  - [StartupDecodingContext](../S/StartupDecodingContext.md): Common decoding context initialization
  - [startup_cb_wrapper](../s/startup_cb_wrapper.md): Output plugin startup callback wrapper

- Called from (representative examples):
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md): During SQL function slot creation
  - [CreateReplicationSlot](CreateReplicationSlot.md): During WAL sender slot creation

## Notes and Other Information
- Must be called within a memory context that outlives the decoding context
- Performs thread-safe plugin name registration using spinlocks
- Implements sophisticated transaction isolation logic with ProcArrayLock coordination
- Supports both automatic WAL reservation and caller-managed WAL retention
- Two-phase commit support is determined by both plugin capabilities and slot configuration
- Returns fully initialized context ready for logical decoding operations
- Includes comprehensive error checking for common misuse scenarios
- Critical for ensuring consistent logical replication slot initialization

## Simplified Source

```c
// Simplified version of CreateInitDecodingContext
LogicalDecodingContext *
CreateInitDecodingContext(const char *plugin,
                         List *output_plugin_options,
                         bool need_full_snapshot,
                         XLogRecPtr restart_lsn,
                         XLogReaderRoutine *xl_routine,
                         LogicalOutputPluginWriterPrepareWrite prepare_write,
                         LogicalOutputPluginWriterWrite do_write,
                         LogicalOutputPluginWriterUpdateProgress update_progress)
{
    TransactionId xmin_horizon = InvalidTransactionId;
    ReplicationSlot *slot = MyReplicationSlot;
    LogicalDecodingContext *ctx;
    MemoryContext old_context;

    // Validate prerequisites and slot state
    CheckLogicalDecodingRequirements();

    if (slot == NULL)
        elog(ERROR, "cannot perform logical decoding without an acquired slot");
    if (plugin == NULL)
        elog(ERROR, "cannot initialize logical decoding without a specified plugin");
    if (SlotIsPhysical(slot))
        ereport(ERROR, "cannot use physical replication slot for logical decoding");
    if (slot->data.database != MyDatabaseId)
        ereport(ERROR, "replication slot was not created in this database");
    if (IsTransactionState() && GetTopTransactionIdIfAny() != InvalidTransactionId)
        ereport(ERROR, "cannot create logical replication slot in transaction that has performed writes");

    // Register plugin name with slot (thread-safe)
    NameData plugin_name;
    namestrcpy(&plugin_name, plugin);
    SpinLockAcquire(&slot->mutex);
    slot->data.plugin = plugin_name;
    SpinLockRelease(&slot->mutex);

    // Handle WAL reservation
    if (XLogRecPtrIsInvalid(restart_lsn))
        ReplicationSlotReserveWal();
    else {
        SpinLockAcquire(&slot->mutex);
        slot->data.restart_lsn = restart_lsn;
        SpinLockRelease(&slot->mutex);
    }

    // Establish safe transaction horizon for decoding
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    xmin_horizon = GetOldestSafeDecodingTransactionId(!need_full_snapshot);

    SpinLockAcquire(&slot->mutex);
    slot->effective_catalog_xmin = xmin_horizon;
    slot->data.catalog_xmin = xmin_horizon;
    if (need_full_snapshot)
        slot->effective_xmin = xmin_horizon;
    SpinLockRelease(&slot->mutex);

    ReplicationSlotsComputeRequiredXmin(true);
    LWLockRelease(ProcArrayLock);

    // Save slot state and create decoding context
    ReplicationSlotMarkDirty();
    ReplicationSlotSave();

    ctx = StartupDecodingContext(NIL, restart_lsn, xmin_horizon,
                                need_full_snapshot, false, true,
                                xl_routine, prepare_write, do_write,
                                update_progress);

    // Initialize output plugin
    old_context = MemoryContextSwitchTo(ctx->context);
    if (ctx->callbacks.startup_cb != NULL)
        startup_cb_wrapper(ctx, &ctx->options, true);
    MemoryContextSwitchTo(old_context);

    // Configure two-phase and rewrite options
    ctx->twophase &= slot->data.two_phase;
    ctx->reorder->output_rewrites = ctx->options.receive_rewrites;

    return ctx;
}
```

Key simplifications made:
- Removed detailed comments for clarity while preserving structure
- Simplified error reporting by removing detailed error codes
- Consolidated spinlock operations for readability
- Abstracted complex xmin management logic into clear steps
- Focused on the main execution path while preserving all critical operations