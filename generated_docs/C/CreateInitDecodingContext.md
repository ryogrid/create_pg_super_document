# CreateInitDecodingContext

## Location
src/backend/replication/logical/logical.c: 332 - 497

## Overview
CreateInitDecodingContext creates and initializes a new logical decoding context for a newly created logical replication slot, including complete setup of the slot's metadata and plugin initialization.

## Definition


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
  - CheckLogicalDecodingRequirements: Validates decoding prerequisites
  - SlotIsPhysical: Checks if slot is physical type
  - IsTransactionState: Validates transaction state
  - GetTopTransactionIdIfAny: Gets active transaction ID if any
  - ReplicationSlotReserveWal: Reserves WAL for slot
  - GetOldestSafeDecodingTransactionId: Calculates safe decoding xmin
  - ReplicationSlotsComputeRequiredXmin: Updates global xmin requirements
  - StartupDecodingContext: Common decoding context initialization
  - startup_cb_wrapper: Output plugin startup callback wrapper

- Called from (representative examples):
  - create_logical_replication_slot: During SQL function slot creation
  - CreateReplicationSlot: During WAL sender slot creation

## Notes and Other Information
- Must be called within a memory context that outlives the decoding context
- Performs thread-safe plugin name registration using spinlocks
- Implements sophisticated transaction isolation logic with ProcArrayLock coordination
- Supports both automatic WAL reservation and caller-managed WAL retention
- Two-phase commit support is determined by both plugin capabilities and slot configuration
- Returns fully initialized context ready for logical decoding operations
- Includes comprehensive error checking for common misuse scenarios
- Critical for ensuring consistent logical replication slot initialization