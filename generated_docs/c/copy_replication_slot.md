# copy_replication_slot

## Location
src/backend/replication/slotfuncs.c: 601 - 857

## Overview
A comprehensive helper function that creates a new replication slot by copying the configuration and state from an existing source slot, supporting both logical and physical replication slots.

## Definition
```c
static Datum copy_replication_slot(FunctionCallInfo fcinfo, bool logical_slot)
```

## Detailed Description
This function implements the complex logic for copying replication slots, handling both logical and physical slot types. It performs a sophisticated two-phase copy operation to ensure consistency while avoiding prolonged locks on the source slot.

The copy process follows these key phases:
1. **Initial snapshot**: Captures the source slot's state under shared lock
2. **Slot creation**: Creates the destination slot with the source's restart LSN
3. **Consistency verification**: Re-reads the source slot to detect any incompatible changes
4. **State synchronization**: Updates the destination slot with the current source values
5. **Validation**: Ensures the copy operation completed successfully without data races

Key safety mechanisms:
- Prevents copying invalid, unfinished, or non-reserved slots
- Validates slot type consistency between source and destination
- Handles concurrent modifications to the source slot during copy
- Ensures WAL availability at the copied restart LSN position
- Maintains global slot accounting (xmin/LSN requirements)

The function deliberately does not copy the failover option to prevent synchronization issues in standby configurations.

## Parameters / Member Variables
- `fcinfo`: Function call information containing:
  - Source slot name (Name/text)
  - Destination slot name (Name/text) 
  - Optional: temporary flag (bool) - overrides source temporariness
  - Optional: plugin name (Name/text) - for logical slots only
- `logical_slot`: Boolean indicating whether to copy as a logical (true) or physical (false) slot

## Dependencies
- Functions called/Symbols referenced:
  - [CheckSlotPermissions](../C/CheckSlotPermissions.md) - Validates user permissions for slot operations
  - `CheckLogicalDecodingRequirements` - Validates logical decoding prerequisites
  - [CheckSlotRequirements](../C/CheckSlotRequirements.md) - Validates general slot requirements
  - [create_logical_replication_slot](create_logical_replication_slot.md) - Creates a new logical replication slot
  - [create_physical_replication_slot](create_physical_replication_slot.md) - Creates a new physical replication slot
  - [ReplicationSlotMarkDirty](../R/ReplicationSlotMarkDirty.md) - Marks the slot for checkpointing
  - [ReplicationSlotsComputeRequiredXmin](../R/ReplicationSlotsComputeRequiredXmin.md) - Recomputes global minimum xmin
  - [ReplicationSlotsComputeRequiredLSN](../R/ReplicationSlotsComputeRequiredLSN.md) - Recomputes global minimum LSN
  - [ReplicationSlotSave](../R/ReplicationSlotSave.md) - Saves slot state to disk
  - [ReplicationSlotPersist](../R/ReplicationSlotPersist.md) - Makes temporary logical slots persistent
  - [ReplicationSlotRelease](../R/ReplicationSlotRelease.md) - Releases the acquired destination slot
- Called from:
  - [pg_copy_logical_replication_slot_a](../p/pg_copy_logical_replication_slot_a.md) - 2-parameter logical slot copy
  - [pg_copy_logical_replication_slot_b](../p/pg_copy_logical_replication_slot_b.md) - 3-parameter logical slot copy  
  - [pg_copy_logical_replication_slot_c](../p/pg_copy_logical_replication_slot_c.md) - 4-parameter logical slot copy
  - [pg_copy_physical_replication_slot_a](../p/pg_copy_physical_replication_slot_a.md) - 2-parameter physical slot copy
  - [pg_copy_physical_replication_slot_b](../p/pg_copy_physical_replication_slot_b.md) - 3-parameter physical slot copy

## Notes and Other Information
- Returns a composite type (slot_name, lsn) with the destination slot name and confirmed flush LSN
- Cannot copy slots that don't reserve WAL or have been invalidated
- Logical slots require a valid confirmed_flush LSN to be copyable
- Source slot type must match the requested destination type
- Uses spinlocks for atomic access to slot data structures
- Includes comprehensive validation to detect concurrent modifications during copy
- Automatically handles both temporary and persistent slot copying
- Physical slots copy restart_lsn as the primary position marker
- Logical slots copy both restart_lsn and confirmed_flush positions
- Thread-safe through careful lock ordering and validation checks
- The destination slot inherits most properties from source but can override temporariness and plugin