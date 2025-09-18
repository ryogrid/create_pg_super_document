# SearchNamedReplicationSlot

## Location
src/backend/replication/slot.c: 464 - 496

## Overview
Searches for a replication slot by name in the shared memory array and returns a pointer to it if found.

## Definition


## Detailed Description
SearchNamedReplicationSlot performs a linear search through the max_replication_slots array in shared memory to find a replication slot with the specified name. The function provides flexible locking behavior based on the need_lock parameter, allowing callers to control whether they need the function to acquire the ReplicationSlotControlLock or if they already hold appropriate locks. The search compares slot names using string comparison and only considers slots that are marked as in_use.

## Parameters / Member Variables
- : The name of the replication slot to search for
- : If true, the function acquires and releases ReplicationSlotControlLock; if false, assumes caller already holds appropriate locks

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease (when need_lock is true)
  - strcmp for name comparison
  - NameStr macro for accessing slot names
- Called from (representative examples):
  - ReplicationSlotAcquire
  - synchronize_one_slot
  - validate_sync_standby_slots
  - StandbySlotsHaveCaughtup

## Notes and Other Information
- Returns NULL if no slot with the specified name is found
- Uses linear search through the replication slots array, which is acceptable given the typically small number of slots
- The need_lock parameter provides flexibility for different calling contexts where locks may already be held
- Only searches slots that are marked as in_use, ignoring freed slots
- Thread-safe when used with appropriate locking (either via need_lock=true or caller-managed locks)