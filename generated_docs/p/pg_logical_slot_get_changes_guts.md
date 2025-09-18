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
  - CheckLogicalDecodingRequirements (verifies logical decoding prerequisites)
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)/ReplicationSlotRelease (slot management)
  - CreateDecodingContext/FreeDecodingContext (decoding context lifecycle)
  - [XLogBeginRead](../X/XLogBeginRead.md)/XLogReadRecord (WAL reading functions)
  - [LogicalDecodingProcessRecord](../L/LogicalDecodingProcessRecord.md) (processes individual WAL records)
  - [LogicalOutputPrepareWrite](../L/LogicalOutputPrepareWrite.md)/LogicalOutputWrite (output handling callbacks)
  - LogicalConfirmReceivedLocation (advances slot position)
  - WaitForStandbyConfirmation (synchronous replication support)
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)/GetXLogReplayRecPtr (determines WAL endpoints)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initializes set-returning function support)
- Data types used:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (main decoding context)
  - [DecodingOutputState](../D/DecodingOutputState.md) (tracks output state and statistics)
  - [XLogRecord](../X/XLogRecord.md) (individual WAL record structure)
  - ReturnSetInfo (set-returning function metadata)
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