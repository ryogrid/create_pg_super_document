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
  - RecoveryInProgress: Checks if server is in recovery mode
  - IsSyncingReplicationSlots: Checks if slot synchronization is active
  - StartupDecodingContext: Common decoding context initialization
  - startup_cb_wrapper: Output plugin startup callback wrapper
  - ReplicationSlotMarkDirty/ReplicationSlotSave: Slot persistence operations
  - SnapBuildSetTwoPhaseAt: Configures two-phase snapshot building

- Called from (representative examples):
  - pg_logical_slot_get_changes_guts: During SQL function change retrieval
  - StartLogicalReplication: During WAL sender logical replication startup
  - LogicalSlotAdvanceAndCheckSnapState: During slot position advancement

## Notes and Other Information
- Handles graceful LSN adjustment when requested position is behind confirmed_flush
- Includes comprehensive slot invalidation detection and error reporting
- Supports two-phase commit with dynamic slot metadata updates
- Fast-forward mode bypasses database validation for performance
- Synchronized slots on standby servers are restricted to synchronization operations only
- Logs detailed startup information including streaming and restart positions
- Critical for resuming logical replication from existing, established slots