# ReplicationSlotAcquire

## Location
src/backend/replication/slot.c: 540 - 651

## Overview
Finds and acquires an existing replication slot by name, marking it as active for the current process.

## Definition


## Detailed Description
ReplicationSlotAcquire locates a replication slot by name and attempts to acquire it for the current process. The function implements both blocking and non-blocking acquisition modes based on the nowait parameter. When nowait is false, the function will wait indefinitely for the slot to become available if it's currently in use by another process. When nowait is true, it immediately errors if the slot is active.

The function uses a combination of lightweight locks and condition variables to coordinate slot access between processes. It employs a retry mechanism for the blocking case, using condition variables to sleep until the owning process releases the slot. Upon successful acquisition, it sets up statistics tracking for logical slots and logs the acquisition for WAL senders.

## Parameters / Member Variables
- : The name of the replication slot to acquire (must not be NULL)
- : If true, error immediately if slot is in use; if false, wait for slot to become available

## Dependencies
- Functions called/Symbols referenced:
  - SearchNamedReplicationSlot
  - LWLockAcquire/LWLockRelease
  - ConditionVariablePrepareToSleep/ConditionVariableSleep/ConditionVariableCancelSleep/ConditionVariableBroadcast
  - SpinLockAcquire/SpinLockRelease
  - SlotIsLogical
  - pgstat_acquire_replslot
- Called from (representative examples):
  - StartReplication
  - StartLogicalReplication
  - pg_logical_slot_get_changes_guts
  - synchronize_one_slot

## Notes and Other Information
- Sets MyReplicationSlot global variable upon successful acquisition
- Uses retry loop with condition variables for blocking acquisition mode
- Resets the slot's inactive_since timestamp when acquired
- Provides different error messages for non-existent vs. in-use slots
- Logs acquisition events for WAL sender processes based on log_replication_commands setting
- Handles both single-user mode (no concurrency checks) and multi-user mode
- Protects against stale statistics from previous slot usage by calling pgstat_acquire_replslot for logical slots