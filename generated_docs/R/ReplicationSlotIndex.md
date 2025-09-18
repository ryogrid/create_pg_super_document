# ReplicationSlotIndex

## Location
[src/backend/replication/slot.c:497-512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L497-L512)

## Overview
Returns the array index of a replication slot within the shared memory replication slots array.

## Definition


## Detailed Description
ReplicationSlotIndex computes and returns the zero-based index of a given ReplicationSlot pointer within the ReplicationSlotCtl->replication_slots array. This function performs pointer arithmetic to determine the slot's position and includes an assertion to validate that the provided slot pointer is within the valid range of the replication slots array. The function is primarily used as an efficient key for storing and accessing replication slot statistics.

## Parameters / Member Variables
- : Pointer to a ReplicationSlot structure that must be within the replication slots array

## Dependencies
- Functions called/Symbols referenced:
  - Assert macro for bounds checking
  - ReplicationSlotCtl global structure access
- Called from (representative examples):
  - pgstat_reset_replslot
  - pgstat_report_replslot
  - pgstat_create_replslot
  - pgstat_acquire_replslot
  - pgstat_drop_replslot

## Notes and Other Information
- Performs bounds checking via assertion to ensure the slot pointer is valid
- Used extensively by the statistics subsystem for efficient slot identification
- The returned index can be used as a key for arrays or hash tables storing per-slot data
- Assumes the slot pointer is valid and within the allocated replication slots array
- Simple pointer arithmetic operation with O(1) time complexity