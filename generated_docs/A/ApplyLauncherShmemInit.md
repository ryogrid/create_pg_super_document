# ApplyLauncherShmemInit

## Location
[src/backend/replication/logical/launcher.c:967-1001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L967-L1001)

## Overview
Allocates and initializes the shared memory structures needed for PostgreSQL's logical replication launcher subsystem.

## Definition
```c
void ApplyLauncherShmemInit(void)
```

## Detailed Description
This function sets up the shared memory infrastructure for the logical replication launcher. It uses the PostgreSQL shared memory management system to either find an existing "Logical Replication Launcher Data" segment or create a new one using the size calculated by ApplyLauncherShmemSize(). When creating new shared memory (when 'found' is false), it initializes the LogicalRepCtx structure, sets up invalid handles for DSA and DSHASH, and initializes all worker slots with proper spin locks. Each worker slot gets its memory zeroed and its relation mutex (relmutex) spin lock initialized.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRepCtxStruct](../L/LogicalRepCtxStruct.md)
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [ApplyLauncherShmemSize](ApplyLauncherShmemSize.md)
  - DSA_HANDLE_INVALID
  - DSHASH_HANDLE_INVALID
  - [LogicalRepWorker](../L/LogicalRepWorker.md)
  - SpinLockInit
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)
  - LOGICALLAUNCHER_H

## Notes and Other Information
- Part of PostgreSQL's shared memory initialization during startup
- Only initializes the structures when creating new shared memory (not when attaching to existing)
- Sets up invalid handles for Dynamic Shared Areas (DSA) and Dynamic Shared Hash tables (DSHASH)
- Initializes spin locks for each worker slot to ensure thread-safe access to worker data
- The function works with the max_logical_replication_workers configuration parameter
- Creates properly aligned and initialized shared memory structures for logical replication workers
- Returns void (no return value)

## Simplified Source

```c
// Simplified version of ApplyLauncherShmemInit
void ApplyLauncherShmemInit(void) {
    bool found;

    // Step 1: Get or create shared memory for logical replication launcher
    LogicalRepCtx = ShmemInitStruct("Logical Replication Launcher Data",
                                   ApplyLauncherShmemSize(), &found);

    // Step 2: Initialize shared memory if newly created
    if (!found) {
        // Clear all memory
        memset(LogicalRepCtx, 0, ApplyLauncherShmemSize());

        // Set invalid handles for dynamic shared areas
        LogicalRepCtx->last_start_dsa = DSA_HANDLE_INVALID;
        LogicalRepCtx->last_start_dsh = DSHASH_HANDLE_INVALID;

        // Step 3: Initialize each worker slot
        for (int slot = 0; slot < max_logical_replication_workers; slot++) {
            LogicalRepWorker *worker = &LogicalRepCtx->workers[slot];

            // Clear worker data and initialize its mutex
            memset(worker, 0, sizeof(LogicalRepWorker));
            SpinLockInit(&worker->relmutex);
        }
    }
}
```

Key simplifications made:
- Removed detailed variable declarations and placed them inline
- Added clear step-by-step comments explaining the initialization process
- Consolidated the worker initialization logic with descriptive comments
- Focused on the main execution path for new shared memory initialization
- Made the three-phase process more explicit: get memory, set handles, initialize workers