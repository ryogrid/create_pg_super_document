# ReplicationSlotIndex

## Location
[src/backend/replication/slot.c:497-512](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L497-L512)

## Overview
Returns the array index of a replication slot within the shared memory replication slots array.

## Definition

```c
int
ReplicationSlotIndex(ReplicationSlot *slot)
```
## Detailed Description
ReplicationSlotIndex computes and returns the zero-based index of a given ReplicationSlot pointer within the ReplicationSlotCtl->replication_slots array. This function performs pointer arithmetic to determine the slot's position and includes an assertion to validate that the provided slot pointer is within the valid range of the replication slots array. The function is primarily used as an efficient key for storing and accessing replication slot statistics.

## Parameters / Member Variables
- `*slot`: Pointer to a ReplicationSlot structure that must be within the replication slots array
## Dependencies
- Functions called/Symbols referenced:
  - Assert macro for bounds checking
  - ReplicationSlotCtl global structure access
- Called from (representative examples):
  - [pgstat_reset_replslot](../p/pgstat_reset_replslot.md)
  - [pgstat_report_replslot](../p/pgstat_report_replslot.md)
  - [pgstat_create_replslot](../p/pgstat_create_replslot.md)
  - [pgstat_acquire_replslot](../p/pgstat_acquire_replslot.md)
  - [pgstat_drop_replslot](../p/pgstat_drop_replslot.md)

## Notes and Other Information
- Performs bounds checking via assertion to ensure the slot pointer is valid
- Used extensively by the statistics subsystem for efficient slot identification
- The returned index can be used as a key for arrays or hash tables storing per-slot data
- Assumes the slot pointer is valid and within the allocated replication slots array
- Simple pointer arithmetic operation with O(1) time complexity

## Simplified Source

```c
// Simplified version of ReplicationSlotIndex
int ReplicationSlotIndex(ReplicationSlot *slot) {
    // Validate that slot pointer is within the valid array bounds
    Assert(slot >= ReplicationSlotCtl->replication_slots &&
           slot < ReplicationSlotCtl->replication_slots + max_replication_slots);

    // Calculate array index using pointer arithmetic
    return slot - ReplicationSlotCtl->replication_slots;
}
```

Key simplifications made:
- Added descriptive comments for the two main operations
- Preserved the essential assertion and pointer arithmetic logic
- Function is already quite simple, so minimal changes were needed
- Maintained the core functionality of computing array index from pointer offset