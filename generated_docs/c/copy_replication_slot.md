# copy_replication_slot

## Location
[src/backend/replication/slotfuncs.c:601-857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slotfuncs.c#L601-L857)

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
  - Optional: temporary flag (bool) - overrides source temporariness
  - Optional: plugin name (Name/text) - for logical slots only
- `logical_slot`: Boolean indicating whether to copy as a logical (true) or physical (false) slot

## Dependencies
- Functions called/Symbols referenced:
  - [CheckSlotPermissions](../C/CheckSlotPermissions.md) - Validates user permissions for slot operations
  - `[CheckLogicalDecodingRequirements](../C/CheckLogicalDecodingRequirements.md)` - Validates logical decoding prerequisites
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

## Simplified Source

```c
static Datum copy_replication_slot(FunctionCallInfo fcinfo, bool logical_slot) {
    // Extract function arguments
    Name src_name = PG_GETARG_NAME(0);
    Name dst_name = PG_GETARG_NAME(1);
    ReplicationSlot *src = NULL;
    ReplicationSlot first_slot_contents, second_slot_contents;
    XLogRecPtr src_restart_lsn;
    bool src_islogical, temporary;
    char *plugin = NULL;

    // Validate function and permissions
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    CheckSlotPermissions();
    if (logical_slot)
        CheckLogicalDecodingRequirements();
    else
        CheckSlotRequirements();

    // Phase 1: Find and snapshot source slot
    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    for (int i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *s = &ReplicationSlotCtl->replication_slots[i];
        if (s->in_use && strcmp(NameStr(s->data.name), NameStr(*src_name)) == 0) {
            SpinLockAcquire(&s->mutex);
            first_slot_contents = *s;
            SpinLockRelease(&s->mutex);
            src = s;
            break;
        }
    }
    LWLockRelease(ReplicationSlotControlLock);

    if (src == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("replication slot \"%s\" does not exist",
                               NameStr(*src_name))));

    // Extract source slot properties
    src_islogical = SlotIsLogical(&first_slot_contents);
    src_restart_lsn = first_slot_contents.data.restart_lsn;
    temporary = (first_slot_contents.data.persistency == RS_TEMPORARY);
    if (logical_slot)
        plugin = NameStr(first_slot_contents.data.plugin);

    // Validate source slot state and type compatibility
    if (src_islogical != logical_slot)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot copy %s slot as %s slot",
                               src_islogical ? "logical" : "physical",
                               logical_slot ? "logical" : "physical")));

    if (XLogRecPtrIsInvalid(src_restart_lsn))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot copy a replication slot that doesn't reserve WAL")));

    if (first_slot_contents.data.invalidated != RS_INVAL_NONE)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("cannot copy invalidated replication slot \"%s\"",
                               NameStr(*src_name))));

    // Override parameters from optional arguments
    if (PG_NARGS() >= 3)
        temporary = PG_GETARG_BOOL(2);
    if (PG_NARGS() >= 4) {
        Assert(logical_slot);
        plugin = NameStr(*(PG_GETARG_NAME(3)));
    }

    // Phase 2: Create destination slot
    if (logical_slot) {
        create_logical_replication_slot(NameStr(*dst_name), plugin,
                                        temporary, false, false,
                                        src_restart_lsn, false);
    } else {
        create_physical_replication_slot(NameStr(*dst_name), true,
                                         temporary, src_restart_lsn);
    }

    // Phase 3: Verify source consistency and update destination
    SpinLockAcquire(&src->mutex);
    second_slot_contents = *src;
    SpinLockRelease(&src->mutex);

    // Validate source slot hasn't changed incompatibly
    XLogRecPtr copy_restart_lsn = second_slot_contents.data.restart_lsn;
    bool copy_islogical = SlotIsLogical(&second_slot_contents);
    char *copy_name = NameStr(second_slot_contents.data.name);

    if (copy_restart_lsn < src_restart_lsn ||
        src_islogical != copy_islogical ||
        strcmp(copy_name, NameStr(*src_name)) != 0) {
        ereport(ERROR, (errmsg("could not copy replication slot \"%s\"",
                               NameStr(*src_name)),
                        errdetail("The source replication slot was modified incompatibly.")));
    }

    // For logical slots, ensure valid confirmed_flush
    if (src_islogical && XLogRecPtrIsInvalid(second_slot_contents.data.confirmed_flush))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot copy unfinished logical replication slot \"%s\"",
                               NameStr(*src_name))));

    // Copy all slot data to destination
    SpinLockAcquire(&MyReplicationSlot->mutex);
    MyReplicationSlot->effective_xmin = second_slot_contents.effective_xmin;
    MyReplicationSlot->effective_catalog_xmin = second_slot_contents.effective_catalog_xmin;
    MyReplicationSlot->data.xmin = second_slot_contents.data.xmin;
    MyReplicationSlot->data.catalog_xmin = second_slot_contents.data.catalog_xmin;
    MyReplicationSlot->data.restart_lsn = second_slot_contents.data.restart_lsn;
    MyReplicationSlot->data.confirmed_flush = second_slot_contents.data.confirmed_flush;
    SpinLockRelease(&MyReplicationSlot->mutex);

    // Persist changes and update global state
    ReplicationSlotMarkDirty();
    ReplicationSlotsComputeRequiredXmin(false);
    ReplicationSlotsComputeRequiredLSN();
    ReplicationSlotSave();

    // Make persistent if needed
    if (logical_slot && !temporary)
        ReplicationSlotPersist();

    // Build return tuple
    Datum values[2];
    bool nulls[2];
    values[0] = NameGetDatum(dst_name);
    nulls[0] = false;

    if (!XLogRecPtrIsInvalid(MyReplicationSlot->data.confirmed_flush)) {
        values[1] = LSNGetDatum(MyReplicationSlot->data.confirmed_flush);
        nulls[1] = false;
    } else {
        nulls[1] = true;
    }

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    ReplicationSlotRelease();

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
```