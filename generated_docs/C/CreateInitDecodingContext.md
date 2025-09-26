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
- : Name of the output plugin to load and initialize
- : Options to pass to the output plugin
- : Whether full table snapshot capability is required
- : WAL position to start from (InvalidXLogRecPtr for auto-selection)
- : WAL reading routine function pointer
- : Callback for preparing output buffer writes
- : Callback for performing actual output writes
- : Callback for progress reporting during decoding

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