# SlotSyncShmemInit

## Location
[src/backend/replication/logical/slotsync.c:1668-1687](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1668-L1687)

## Overview
Allocates and initializes the shared memory structure used for coordinating slot synchronization between processes.

## Definition
```c
void SlotSyncShmemInit(void)
```

## Detailed Description
This function sets up the shared memory infrastructure required for slot synchronization by allocating and initializing the `SlotSyncCtxStruct`. It uses PostgreSQL's shared memory management system to either attach to an existing shared memory segment or create a new one if this is the first process to access it.

When creating a new shared memory segment, the function initializes all fields to safe default values: it zeros out the entire structure, sets the process ID to `InvalidPid` (indicating no active slot sync worker), and initializes the spinlock used for coordinating access to the shared data.

The shared memory structure contains critical coordination information including the slot sync worker's process ID, synchronization flags, timing information, and mutual exclusion primitives needed for safe concurrent access.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [SlotSyncShmemSize](SlotSyncShmemSize.md) (to determine memory size)
  - [SlotSyncCtxStruct](SlotSyncCtxStruct.md) (structure type)
  - [ShmemInitStruct](ShmemInitStruct.md) (shared memory allocation)
  - InvalidPid (constant for invalid process ID)
  - SpinLockInit (spinlock initialization)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)

## Notes and Other Information
- Must be called after `SlotSyncShmemSize` has been used to calculate required memory during startup
- Uses the "Slot Sync Data" identifier for the shared memory segment
- Handles both initial creation and subsequent attachments to existing shared memory
- Initializes the mutex/spinlock for thread-safe access to shared state
- Part of PostgreSQL's startup sequence for shared memory initialization
- Located in src/backend/replication/logical/slotsync.c:1664-1682

## Simplified Source

```c
// Simplified version of SlotSyncShmemInit
void SlotSyncShmemInit(void) {
    // Step 1: Calculate required memory size
    Size size = SlotSyncShmemSize();
    bool found;

    // Step 2: Allocate or attach to shared memory segment
    SlotSyncCtx = (SlotSyncCtxStruct *)
        ShmemInitStruct("Slot Sync Data", size, &found);

    // Step 3: Initialize shared memory if newly created
    if (!found) {
        // Clear all memory to zero
        memset(SlotSyncCtx, 0, size);

        // Set safe initial values
        SlotSyncCtx->pid = InvalidPid;      // No active worker
        SpinLockInit(&SlotSyncCtx->mutex);  // Initialize mutex
    }
}
```

Key simplifications made:
- Added step-by-step comments explaining the main phases
- Clarified the purpose of each initialization operation
- Focused on the essential shared memory setup logic
- Highlighted the conditional initialization for new memory segments