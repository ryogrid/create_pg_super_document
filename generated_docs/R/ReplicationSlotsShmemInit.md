# ReplicationSlotsShmemInit

## Location
[src/backend/replication/slot.c:189-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L189-L223)

## Overview
Allocates and initializes shared memory for the replication slot subsystem, setting up the control structure and individual slot synchronization primitives.

## Definition
```c
void ReplicationSlotsShmemInit(void)
```

## Detailed Description
This function initializes the shared memory segment for replication slots during PostgreSQL startup. It allocates a shared memory block of the size determined by ReplicationSlotsShmemSize() and initializes the ReplicationSlotCtl global control structure. If this is the first time the shared memory is being created (not found), it zeros out the entire structure and initializes synchronization primitives for each replication slot including spinlocks, lightweight locks, and condition variables.

The function performs different actions based on whether the shared memory structure already exists:
- If found (restart): Just attaches to existing shared memory
- If not found (first initialization): Zeros memory and initializes all slot synchronization primitives

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [ReplicationSlotsShmemSize](ReplicationSlotsShmemSize.md)
  - MemSet
  - SpinLockInit
  - LWLockInitialize
  - [ConditionVariableInit](../C/ConditionVariableInit.md)
  - [ReplicationSlotCtlData](ReplicationSlotCtlData.md) (struct)
  - [ReplicationSlot](ReplicationSlot.md) (struct)
  - LWTRANCHE_REPLICATION_SLOT_IO (constant)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)

## Notes and Other Information
- Returns immediately if max_replication_slots is 0 (replication slots disabled)
- Initializes three types of synchronization primitives per slot: spinlock (mutex), lightweight lock (io_in_progress_lock), and condition variable (active_cv)
- The io_in_progress_lock uses the LWTRANCHE_REPLICATION_SLOT_IO tranche for lock wait event tracking
- This function is called once during PostgreSQL startup as part of shared memory initialization