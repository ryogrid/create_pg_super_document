# CreateReplicationSlot

## Location
[src/bin/pg_basebackup/streamutil.c:655-762](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L655-L762)

## Overview
Creates a new replication slot (physical or logical) for PostgreSQL streaming replication, handling slot initialization, snapshot management, and returning slot information to the client.

## Definition

```c
bool
CreateReplicationSlot(PGconn *conn, const char *slot_name, const char *plugin,
					  bool is_temporary, bool is_physical, bool reserve_wal,
					  bool slot_exists_ok, bool two_phase)
```
## Detailed Description
This function creates a new replication slot based on the command parameters. For physical slots, it creates the slot and optionally reserves WAL to prevent it from being recycled. For logical slots, it performs additional setup including logical decoding context initialization, snapshot handling (export/use), and plugin validation. The function validates transaction state requirements for logical slots with snapshot operations and returns detailed information about the created slot including its name, consistent point, snapshot name (if applicable), and output plugin.

## Parameters / Member Variables
- `cmd`: CreateReplicationSlotCmd structure containing slot creation parameters including:
  - `slotname`: Name for the new replication slot
  - `kind`: Type of slot (REPLICATION_KIND_PHYSICAL or REPLICATION_KIND_LOGICAL)
  - `plugin`: Output plugin name (for logical slots only)
  - `temporary`: Whether the slot should be temporary (RS_TEMPORARY) or persistent

## Dependencies
- Functions called/Symbols referenced:
  - [parseCreateReplSlotOptions](../p/parseCreateReplSlotOptions.md) - Parse slot creation options
  - [ReplicationSlotCreate](../R/ReplicationSlotCreate.md) - Create the actual replication slot
  - [ReplicationSlotReserveWal](../R/ReplicationSlotReserveWal.md) - Reserve WAL for physical slots
  - [ReplicationSlotMarkDirty](../R/ReplicationSlotMarkDirty.md) - Mark slot as needing persistence
  - [ReplicationSlotSave](../R/ReplicationSlotSave.md) - Save persistent slot to disk
  - [CheckLogicalDecodingRequirements](CheckLogicalDecodingRequirements.md) - Validate logical decoding setup
  - [CreateInitDecodingContext](CreateInitDecodingContext.md) - [Initialize](../I/Initialize.md) logical decoding context
  - [DecodingContextFindStartpoint](../D/DecodingContextFindStartpoint.md) - Build initial snapshot and find start point
  - [SnapBuildExportSnapshot](../S/SnapBuildExportSnapshot.md) - Export snapshot for logical slots
  - [SnapBuildInitialSnapshot](../S/SnapBuildInitialSnapshot.md) - Create initial snapshot
  - [RestoreTransactionSnapshot](../R/RestoreTransactionSnapshot.md) - Apply snapshot to current transaction
  - [FreeDecodingContext](../F/FreeDecodingContext.md) - Clean up decoding context
  - [ReplicationSlotPersist](../R/ReplicationSlotPersist.md) - Make ephemeral slot persistent
  - [CreateDestReceiver](CreateDestReceiver.md) - Create output destination
  - [CreateTemplateTupleDesc](CreateTemplateTupleDesc.md) - Create tuple descriptor for results
  - Various tuple output functions for returning results
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md) - Release the created slot
- Called from (representative examples):
  - [exec_replication_command](../e/exec_replication_command.md) (walsender.c:2131)
  - [StartLogStreamer](../S/StartLogStreamer.md) (pg_basebackup.c:669)
  - [main](../m/main.md) functions in pg_receivewal.c and pg_recvlogical.c

## Notes and Other Information
- Physical slots can optionally reserve WAL to prevent cleanup before the slot is used
- Logical slots require additional validation of transaction state and isolation level
- [Snapshot](../S/Snapshot.md) export/use operations have strict transaction requirements (must be outside/inside transaction respectively)
- Logical slots are initially created as ephemeral and converted to persistent after successful initialization
- The function returns a 4-column result set: slot_name, consistent_point, snapshot_name, output_plugin
- For snapshot 'use' operations, the transaction must be REPEATABLE READ, read-only, and called before any other queries
- Two-phase and failover support are configurable options for logical slots
- The consistent_point returned is the confirmed_flush LSN formatted as X/X

## Simplified Source

```c
// Simplified version of CreateReplicationSlot (walsender.c)
static void
CreateReplicationSlot(CreateReplicationSlotCmd *cmd) {
    const char *snapshot_name = NULL;
    char xloc[MAXFNAMELEN];
    char *slot_name;
    bool reserve_wal = false;
    bool two_phase = false;
    bool failover = false;
    CRSSnapshotAction snapshot_action = CRS_EXPORT_SNAPSHOT;

    // Parse command options
    parseCreateReplSlotOptions(cmd, &reserve_wal, &snapshot_action, &two_phase, &failover);

    if (cmd->kind == REPLICATION_KIND_PHYSICAL) {
        // Create physical replication slot
        ReplicationSlotCreate(cmd->slotname, false,
                             cmd->temporary ? RS_TEMPORARY : RS_PERSISTENT,
                             false, false, false);

        // Optionally reserve WAL to prevent cleanup
        if (reserve_wal) {
            ReplicationSlotReserveWal();
            ReplicationSlotMarkDirty();

            // Save permanent slots to disk
            if (!cmd->temporary)
                ReplicationSlotSave();
        }
    } else {
        // Create logical replication slot
        LogicalDecodingContext *ctx;
        bool need_full_snapshot = false;

        // Validate logical decoding requirements
        CheckLogicalDecodingRequirements();

        // Create slot initially as ephemeral for error handling
        ReplicationSlotCreate(cmd->slotname, true,
                             cmd->temporary ? RS_TEMPORARY : RS_EPHEMERAL,
                             two_phase, failover, false);

        // Validate snapshot action requirements
        if (snapshot_action == CRS_EXPORT_SNAPSHOT) {
            // Must be outside transaction
            if (IsTransactionBlock())
                ereport(ERROR, "CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export') must not be called inside a transaction");
            need_full_snapshot = true;
        } else if (snapshot_action == CRS_USE_SNAPSHOT) {
            // Must be inside REPEATABLE READ read-only transaction
            if (!IsTransactionBlock() || XactIsoLevel != XACT_REPEATABLE_READ || !XactReadOnly)
                ereport(ERROR, "Invalid transaction state for SNAPSHOT 'use'");
            need_full_snapshot = true;
        }

        // Initialize logical decoding context
        ctx = CreateInitDecodingContext(cmd->plugin, NIL, need_full_snapshot,
                                       InvalidXLogRecPtr, /* WAL routines */,
                                       WalSndPrepareWrite, WalSndWriteData,
                                       WalSndUpdateProgress);

        // Disable timeout during slot creation
        last_reply_timestamp = 0;

        // Build initial snapshot and find start point
        DecodingContextFindStartpoint(ctx);

        // Handle snapshot export/use
        if (snapshot_action == CRS_EXPORT_SNAPSHOT) {
            snapshot_name = SnapBuildExportSnapshot(ctx->snapshot_builder);
        } else if (snapshot_action == CRS_USE_SNAPSHOT) {
            Snapshot snap = SnapBuildInitialSnapshot(ctx->snapshot_builder);
            RestoreTransactionSnapshot(snap, MyProc);
        }

        // Clean up decoding context
        FreeDecodingContext(ctx);

        // Make slot persistent if not temporary
        if (!cmd->temporary)
            ReplicationSlotPersist();
    }

    // Format consistent point LSN
    snprintf(xloc, sizeof(xloc), "%X/%X",
             LSN_FORMAT_ARGS(MyReplicationSlot->data.confirmed_flush));

    // Prepare result tuple with 4 columns: slot_name, consistent_point, snapshot_name, output_plugin
    DestReceiver *dest = CreateDestReceiver(DestRemoteSimple);
    TupleDesc tupdesc = CreateTemplateTupleDesc(4);

    // Initialize tuple descriptor columns
    TupleDescInitBuiltinEntry(tupdesc, 1, "slot_name", TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 2, "consistent_point", TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 3, "snapshot_name", TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 4, "output_plugin", TEXTOID, -1, 0);

    // Prepare and send result tuple
    TupOutputState *tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

    Datum values[4];
    bool nulls[4] = {0};

    // Fill result values
    values[0] = CStringGetTextDatum(NameStr(MyReplicationSlot->data.name));
    values[1] = CStringGetTextDatum(xloc);
    values[2] = snapshot_name ? CStringGetTextDatum(snapshot_name) : (nulls[2] = true, 0);
    values[3] = cmd->plugin ? CStringGetTextDatum(cmd->plugin) : (nulls[3] = true, 0);

    // Send result and cleanup
    do_tup_output(tstate, values, nulls);
    end_tup_output(tstate);
    ReplicationSlotRelease();
}
```

Key simplifications made:
- Removed detailed error messages for clarity, kept essential error conditions
- Consolidated transaction state validation for snapshot 'use' operations
- Abstracted WAL routine structure initialization details
- Simplified tuple output preparation and combined variable declarations
- Focused on the main execution paths for physical vs logical slots
- Reduced verbose comments while preserving critical logic flow explanations