# AsyncShmemInit

## Location
[src/backend/commands/async.c:502-556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L502-L556)

## Overview
Initializes the shared memory structures and SLRU (Simple Least Recently Used) management for PostgreSQL's asynchronous notification system.

## Definition
```c
void AsyncShmemInit(void)
```

## Detailed Description
The `AsyncShmemInit` function performs the crucial initialization of shared memory components for the LISTEN/NOTIFY asynchronous messaging system. It creates or attaches to the AsyncQueueControl structure, which manages the notification queue and backend status information. When initializing for the first time (indicated by `!found`), it sets up initial queue positions, clears backend tracking arrays, and initializes the SLRU buffer management system. The function also configures the pg_notify SLRU system with appropriate page precedence logic and cleans up any existing notification files during startup.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [mul_size](../m/mul_size.md), add_size (safe arithmetic for memory calculations)
  - [ShmemInitStruct](../S/ShmemInitStruct.md) (shared memory structure initialization)
  - SET_QUEUE_POS (macro for setting queue positions)
  - [asyncQueuePagePrecedes](../a/asyncQueuePagePrecedes.md) (page ordering function)
  - [SimpleLruInit](../S/SimpleLruInit.md) (SLRU system initialization)
  - [SlruScanDirectory](../S/SlruScanDirectory.md), SlruScanDirCbDeleteAll (SLRU directory management)
  - [AsyncQueueControl](AsyncQueueControl.md), QueueBackendStatus (data structures)
  - Various queue management macros (QUEUE_HEAD, QUEUE_TAIL, etc.)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (during shared memory setup)
  - Referenced in ASYNC_H header file

## Notes and Other Information
- Must be called during PostgreSQL startup to establish the notification system
- Uses the same size calculations as AsyncShmemSize to ensure consistency
- Initializes queue head and tail positions to (0,0) for a fresh start
- Sets up backend tracking arrays for MaxBackends processes
- Configures SLRU with long segment names to avoid wraparound issues
- Cleans out the pg_notify directory during fresh initialization to ensure a clean state
- Critical component of the shared memory initialization sequence
- The `found` parameter from ShmemInitStruct indicates whether this is the first process to initialize the structure

## Simplified Source

```c
// Simplified version of AsyncShmemInit
void AsyncShmemInit(void) {
    bool found;
    Size size;

    // Calculate shared memory size for queue control structure
    size = mul_size(MaxBackends, sizeof(QueueBackendStatus));
    size = add_size(size, offsetof(AsyncQueueControl, backend));

    // Create or attach to the async queue control structure
    asyncQueueControl = (AsyncQueueControl *)
        ShmemInitStruct("Async Queue Control", size, &found);

    if (!found) {
        // First-time initialization: reset queue positions
        SET_QUEUE_POS(QUEUE_HEAD, 0, 0);
        SET_QUEUE_POS(QUEUE_TAIL, 0, 0);
        QUEUE_STOP_PAGE = 0;
        QUEUE_FIRST_LISTENER = INVALID_PROC_NUMBER;
        asyncQueueControl->lastQueueFillWarn = 0;

        // Initialize all backend status entries
        for (int i = 0; i < MaxBackends; i++) {
            QUEUE_BACKEND_PID(i) = InvalidPid;
            QUEUE_BACKEND_DBOID(i) = InvalidOid;
            QUEUE_NEXT_LISTENER(i) = INVALID_PROC_NUMBER;
            SET_QUEUE_POS(QUEUE_BACKEND_POS(i), 0, 0);
        }
    }

    // Initialize SLRU management for pg_notify data
    NotifyCtl->PagePrecedes = asyncQueuePagePrecedes;
    SimpleLruInit(NotifyCtl, "notify", notify_buffers, 0,
                  "pg_notify", LWTRANCHE_NOTIFY_BUFFER, LWTRANCHE_NOTIFY_SLRU,
                  SYNC_HANDLER_NONE, true);

    if (!found) {
        // Clean out pg_notify directory on fresh start
        SlruScanDirectory(NotifyCtl, SlruScanDirCbDeleteAll, NULL);
    }
}
```

Key simplifications made:
- Preserved the essential two-phase initialization logic (create/attach, then configure)
- Maintained the critical first-time setup conditional block
- Kept all essential queue and backend initialization steps
- Preserved the SLRU initialization and cleanup logic
- Added descriptive comments for each major section
- Maintained the original function structure and flow