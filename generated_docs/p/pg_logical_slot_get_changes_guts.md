# pg_logical_slot_get_changes_guts

## Location
[src/backend/replication/logical/logicalfuncs.c:99-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logicalfuncs.c#L99-L330)

## Overview
Core helper function that implements the main logic for SQL-callable logical decoding functions, handling the complete process of retrieving and formatting logical replication changes from a replication slot.

## Definition
```c
static Datum pg_logical_slot_get_changes_guts(FunctionCallInfo fcinfo, bool confirm, bool binary)
```

## Detailed Description
This is the central implementation function for PostgreSQL logical replication SQL interface. It performs comprehensive logical decoding by reading WAL records from a replication slot, processing them through the logical decoding subsystem, and returning the results in a structured format suitable for SQL consumption. The function handles parameter validation, slot management, WAL reading, change decoding, output formatting, and proper cleanup with robust error handling.

The function supports both text and binary output modes, handles various limits (LSN and row count), manages memory contexts appropriately, and ensures proper transaction state management throughout the decoding process.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo structure containing SQL function call information and parameters
- `confirm`: Boolean flag indicating whether to confirm processed LSN positions (advance the slot)  
- `binary`: Boolean flag specifying output format (true for binary, false for textual)

## Dependencies
- Functions called/Symbols referenced:
  - [CheckSlotPermissions](../C/CheckSlotPermissions.md) (validates slot access permissions)
  - [CheckLogicalDecodingRequirements](../C/CheckLogicalDecodingRequirements.md) (verifies logical decoding prerequisites)
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)/ReplicationSlotRelease (slot management)
  - [CreateDecodingContext](../C/CreateDecodingContext.md)/FreeDecodingContext (decoding context lifecycle)
  - [XLogBeginRead](../X/XLogBeginRead.md)/XLogReadRecord (WAL reading functions)
  - [LogicalDecodingProcessRecord](../L/LogicalDecodingProcessRecord.md) (processes individual WAL records)
  - [LogicalOutputPrepareWrite](../L/LogicalOutputPrepareWrite.md)/LogicalOutputWrite (output handling callbacks)
  - [LogicalConfirmReceivedLocation](../L/LogicalConfirmReceivedLocation.md) (advances slot position)
  - [WaitForStandbyConfirmation](../W/WaitForStandbyConfirmation.md) (synchronous replication support)
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)/GetXLogReplayRecPtr (determines WAL endpoints)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initializes set-returning function support)
- Data types used:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (main decoding context)
  - [DecodingOutputState](../D/DecodingOutputState.md) (tracks output state and statistics)
  - [XLogRecord](../X/XLogRecord.md) (individual WAL record structure)
  - [ReturnSetInfo](../R/ReturnSetInfo.md) (set-returning function metadata)
- Called from:
  - [pg_logical_slot_get_changes](pg_logical_slot_get_changes.md) (public SQL function for textual output with confirmation)
  - [pg_logical_slot_peek_changes](pg_logical_slot_peek_changes.md) (public SQL function for textual output without confirmation)  
  - [pg_logical_slot_get_binary_changes](pg_logical_slot_get_binary_changes.md) (public SQL function for binary output with confirmation)
  - [pg_logical_slot_peek_binary_changes](pg_logical_slot_peek_binary_changes.md) (public SQL function for binary output without confirmation)

## Notes and Other Information
- This is a static function serving as the implementation core for multiple public SQL functions
- Implements comprehensive error handling with PG_TRY/PG_CATCH blocks for proper cleanup
- Supports parameter-based limits: maximum LSN position and maximum number of changes
- Handles both recovery and normal operation modes with appropriate WAL endpoint detection
- Manages memory contexts to ensure proper memory management during long-running operations
- Includes validation for output plugin compatibility (textual vs binary output)
- Processes options array for configuring logical decoding behavior
- Maintains replication slot state and advances confirmed_flush position when requested
- Includes system cache invalidation for proper catalog visibility during decoding
- Located in src/backend/replication/logical/logicalfuncs.c:99-330

## Simplified Source

```c
static Datum pg_logical_slot_get_changes_guts(FunctionCallInfo fcinfo, bool confirm, bool binary)
{
    Name name;
    XLogRecPtr upto_lsn;
    int32 upto_nchanges;
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    XLogRecPtr end_of_wal;
    LogicalDecodingContext *ctx;
    ResourceOwner old_resowner = CurrentResourceOwner;
    List *options = NIL;
    DecodingOutputState *p;

    // Validate permissions and requirements
    CheckSlotPermissions();
    CheckLogicalDecodingRequirements();

    // Extract function arguments
    name = PG_GETARG_NAME(0);
    upto_lsn = PG_ARGISNULL(1) ? InvalidXLogRecPtr : PG_GETARG_LSN(1);
    upto_nchanges = PG_ARGISNULL(2) ? InvalidXLogRecPtr : PG_GETARG_INT32(2);

    // Process options array (simplified error handling)
    ArrayType *arr = PG_GETARG_ARRAYTYPE_P(3);
    // ... options processing logic ...

    // Initialize output state
    p = palloc0(sizeof(DecodingOutputState));
    p->binary_output = binary;

    InitMaterializedSRF(fcinfo, 0);
    p->tupstore = rsinfo->setResult;
    p->tupdesc = rsinfo->setDesc;

    // Determine end of WAL
    end_of_wal = RecoveryInProgress() ? GetXLogReplayRecPtr(NULL) : GetFlushRecPtr(NULL);

    // Acquire replication slot
    ReplicationSlotAcquire(NameStr(*name), true);

    PG_TRY();
    {
        // Create decoding context
        ctx = CreateDecodingContext(InvalidXLogRecPtr, options, false,
                                    XL_ROUTINE(.page_read = read_local_xlog_page,
                                               .segment_open = wal_segment_open,
                                               .segment_close = wal_segment_close),
                                    LogicalOutputPrepareWrite,
                                    LogicalOutputWrite, NULL);

        // Validate output plugin compatibility
        if (!binary && ctx->options.output_type != OUTPUT_PLUGIN_TEXTUAL_OUTPUT)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("output plugin produces binary output, but function expects textual data")));

        // Wait for standby confirmation
        XLogRecPtr wait_for_wal_lsn = XLogRecPtrIsInvalid(upto_lsn) ? end_of_wal : Min(upto_lsn, end_of_wal);
        WaitForStandbyConfirmation(wait_for_wal_lsn);

        ctx->output_writer_private = p;

        // Start reading from slot's restart_lsn
        XLogBeginRead(ctx->reader, MyReplicationSlot->data.restart_lsn);
        InvalidateSystemCaches();

        // Main decoding loop
        while (ctx->reader->EndRecPtr < end_of_wal)
        {
            XLogRecord *record;
            char *errm = NULL;

            record = XLogReadRecord(ctx->reader, &errm);
            if (errm)
                elog(ERROR, "could not find record for logical decoding: %s", errm);

            if (record != NULL)
                LogicalDecodingProcessRecord(ctx, ctx->reader);

            // Check limits
            if (upto_lsn != InvalidXLogRecPtr && upto_lsn <= ctx->reader->EndRecPtr)
                break;
            if (upto_nchanges != 0 && upto_nchanges <= p->returned_rows)
                break;

            CHECK_FOR_INTERRUPTS();
        }

        CurrentResourceOwner = old_resowner;

        // Confirm processed position if requested
        if (ctx->reader->EndRecPtr != InvalidXLogRecPtr && confirm)
        {
            LogicalConfirmReceivedLocation(ctx->reader->EndRecPtr);
            ReplicationSlotMarkDirty();
        }

        // Cleanup
        FreeDecodingContext(ctx);
        ReplicationSlotRelease();
        InvalidateSystemCaches();
    }
    PG_CATCH();
    {
        InvalidateSystemCaches();
        PG_RE_THROW();
    }
    PG_END_TRY();

    return (Datum) 0;
}
```