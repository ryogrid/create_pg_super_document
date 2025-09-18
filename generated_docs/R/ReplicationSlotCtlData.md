# ReplicationSlotCtlData

## Location
src/include/replication/slot.h: 219 - 226

## Overview
ReplicationSlotCtlData is the shared memory control structure that manages the array of all replication slots in the PostgreSQL system, serving as the top-level container for slot management.

## Definition
```c
typedef struct ReplicationSlotCtlData
{
    /*
     * This array should be declared [FLEXIBLE_ARRAY_MEMBER], but for some
     * reason you can't do that in an otherwise-empty struct.
     */
    ReplicationSlot replication_slots[1];
} ReplicationSlotCtlData;
```

## Detailed Description
This structure serves as the shared memory control area for managing all replication slots in the PostgreSQL instance. It contains an array of ReplicationSlot structures that represents the complete set of available slots. The structure uses a flexible array member pattern where the array size is determined at runtime during shared memory initialization based on the max_replication_slots configuration parameter.

## Parameters / Member Variables
- `replication_slots[1]`: Array of ReplicationSlot structures representing all available replication slots. Despite being declared as size 1, this acts as a flexible array member that is properly sized during shared memory allocation to accommodate the configured maximum number of replication slots.

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlot](ReplicationSlot.md) (array element type)
- Called from (representative examples):
  - [ReplicationSlotsShmemSize](ReplicationSlotsShmemSize.md) (for calculating shared memory requirements)
  - [ReplicationSlotsShmemInit](ReplicationSlotsShmemInit.md) (for initializing the control structure)
  - SLOT_VERSION (version-related operations)

## Notes and Other Information
The structure uses a workaround for C language limitations where flexible array members cannot be declared in otherwise-empty structs. The actual array size is calculated during shared memory initialization and is based on the max_replication_slots GUC parameter. This design pattern allows PostgreSQL to allocate exactly the right amount of shared memory for the configured number of replication slots while maintaining type safety and proper memory layout. The structure is part of PostgreSQL's shared memory architecture and is accessed by multiple backend processes concurrently under appropriate locking protocols.