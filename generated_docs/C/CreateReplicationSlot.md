# CreateReplicationSlot

## Location
src/bin/pg_basebackup/streamutil.c: 655 - 762

## Overview
Creates a new replication slot (physical or logical) for PostgreSQL streaming replication, handling slot initialization, snapshot management, and returning slot information to the client.

## Definition


## Detailed Description
This function creates a new replication slot based on the command parameters. For physical slots, it creates the slot and optionally reserves WAL to prevent it from being recycled. For logical slots, it performs additional setup including logical decoding context initialization, snapshot handling (export/use), and plugin validation. The function validates transaction state requirements for logical slots with snapshot operations and returns detailed information about the created slot including its name, consistent point, snapshot name (if applicable), and output plugin.

## Parameters / Member Variables
- `cmd`: CreateReplicationSlotCmd structure containing slot creation parameters including:
  - `slotname`: Name for the new replication slot
  - `kind`: Type of slot (REPLICATION_KIND_PHYSICAL or REPLICATION_KIND_LOGICAL)
  - `plugin`: Output plugin name (for logical slots only)
  - `temporary`: Whether the slot should be temporary (RS_TEMPORARY) or persistent
  - Additional options for WAL reservation, snapshot actions, two-phase support, and failover

## Dependencies
- Functions called/Symbols referenced:
  - parseCreateReplSlotOptions - Parse slot creation options
  - ReplicationSlotCreate - Create the actual replication slot
  - ReplicationSlotReserveWal - Reserve WAL for physical slots
  - ReplicationSlotMarkDirty - Mark slot as needing persistence
  - ReplicationSlotSave - Save persistent slot to disk
  - CheckLogicalDecodingRequirements - Validate logical decoding setup
  - CreateInitDecodingContext - Initialize logical decoding context
  - DecodingContextFindStartpoint - Build initial snapshot and find start point
  - SnapBuildExportSnapshot - Export snapshot for logical slots
  - SnapBuildInitialSnapshot - Create initial snapshot
  - RestoreTransactionSnapshot - Apply snapshot to current transaction
  - FreeDecodingContext - Clean up decoding context
  - ReplicationSlotPersist - Make ephemeral slot persistent
  - CreateDestReceiver - Create output destination
  - CreateTemplateTupleDesc - Create tuple descriptor for results
  - Various tuple output functions for returning results
  - ReplicationSlotRelease - Release the created slot
- Called from (representative examples):
  - exec_replication_command (walsender.c:2131)
  - StartLogStreamer (pg_basebackup.c:669)
  - main functions in pg_receivewal.c and pg_recvlogical.c

## Notes and Other Information
- Physical slots can optionally reserve WAL to prevent cleanup before the slot is used
- Logical slots require additional validation of transaction state and isolation level
- Snapshot export/use operations have strict transaction requirements (must be outside/inside transaction respectively)
- Logical slots are initially created as ephemeral and converted to persistent after successful initialization
- The function returns a 4-column result set: slot_name, consistent_point, snapshot_name, output_plugin
- For snapshot 'use' operations, the transaction must be REPEATABLE READ, read-only, and called before any other queries
- Two-phase and failover support are configurable options for logical slots
- The consistent_point returned is the confirmed_flush LSN formatted as X/X