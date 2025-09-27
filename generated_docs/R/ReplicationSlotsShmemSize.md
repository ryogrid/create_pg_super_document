# ReplicationSlotsShmemSize

## Location
[src/backend/replication/slot.c:171-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L171-L188)

## Overview
Calculates the shared memory space required for the replication slot subsystem based on the configured maximum number of replication slots.

## Definition
```c
Size ReplicationSlotsShmemSize(void)
```

## Detailed Description
This function computes the total shared memory space needed by the replication slot subsystem. It takes into account the base size of the ReplicationSlotCtlData structure and adds space for the configured maximum number of replication slots (max_replication_slots). If max_replication_slots is 0 (replication slots disabled), it returns 0 to indicate no shared memory is needed.

The calculation includes:
- Base offset to the replication_slots array within ReplicationSlotCtlData
- Space for max_replication_slots number of ReplicationSlot structures

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [add_size](../a/add_size.md)
  - [mul_size](../m/mul_size.md)
  - [ReplicationSlotCtlData](ReplicationSlotCtlData.md) (struct)
  - [ReplicationSlot](ReplicationSlot.md) (struct)
- Called from (representative examples):
  - [CalculateShmemSize](../C/CalculateShmemSize.md)
  - [ReplicationSlotsShmemInit](ReplicationSlotsShmemInit.md)

## Notes and Other Information
- Returns 0 if max_replication_slots is 0, effectively disabling replication slot shared memory allocation
- Uses PostgreSQL's safe arithmetic functions (add_size, mul_size) to prevent integer overflow
- This function is called during PostgreSQL startup to determine total shared memory requirements

## Simplified Source

```c
// Simplified version of ReplicationSlotsShmemSize
Size ReplicationSlotsShmemSize(void) {
    Size size = 0;

    // Early return if replication slots are disabled
    if (max_replication_slots == 0)
        return size;

    // Calculate base size: offset to replication_slots array
    size = offsetof(ReplicationSlotCtlData, replication_slots);

    // Add space for all replication slot structures
    size = add_size(size, mul_size(max_replication_slots, sizeof(ReplicationSlot)));

    return size;
}
```

Key simplifications made:
- Preserved the essential memory calculation logic
- Kept the early return for disabled replication slots
- Maintained the safe arithmetic operations for overflow protection
- Added clear comments explaining each calculation step